#pragma once

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <cstddef>
#include <exception>
#include <thread>
#include <utility>
#include "SocketResourceBudget.h"

// One asynchronous execution owner for a RedSocket instance. Socket, resolver,
// and operation handlers must be constructed on this executor and every
// initiating/cancel/close operation is serialized by the owner. The
// exceptional post-join drain requires all epochs already invalidated.
class SocketOwner {
public:
    using Executor = asio::strand<asio::io_context::executor_type>;
    static constexpr std::size_t kMaxQueuedTasks = 4096;
    static constexpr std::size_t kMaxQueuedBytes = 8 * 1024 * 1024;
    static constexpr std::size_t kMaxCriticalTasks = 16;

    SocketOwner() :
        m_executor(asio::make_strand(m_context)),
        m_work(asio::make_work_guard(m_context)),
        m_workerExited(m_workerExitedPromise.get_future().share()),
        m_worker([this] {
            for (;;) {
                try {
                    m_context.run();
                    break;
                } catch (const std::exception&) {
                    m_workerFailed.store(true, std::memory_order_release);
                } catch (...) {
                    m_workerFailed.store(true, std::memory_order_release);
                }
            }
            try { m_workerExitedPromise.set_value(); } catch (...) {}
        }) {
    }

    SocketOwner(const SocketOwner&) = delete;
    SocketOwner& operator=(const SocketOwner&) = delete;

    ~SocketOwner() {
        if (!Stop([] {})) {
            // A normal Stop may return after its diagnostic timeout while a
            // slow cooperative handler is still unwinding. Destruction must
            // not leave std::thread joinable or free executor state under it.
            // It therefore waits without a hard bound; arbitrary non-returning
            // handlers remain outside any truthful timeout guarantee.
            if (IsWorkerThread()) {
                // Destroying the owner from inside a task cannot be made safe
                // by joining the current thread; callers must request Stop
                // there and let an external lifetime owner reap it.
                std::terminate();
            }
            m_workerExited.wait();
            std::lock_guard joinLock(m_joinMutex);
            if (m_worker.joinable()) m_worker.join();
        }
    }

#if defined(DEEPBIDE_NATIVE_TEST)
    void FailNextPostForTest() noexcept { m_failPost.store(true); }
    void FailNextFinalizerForTest() noexcept { m_failFinalizer.store(true); }
    bool AbortingForTest() const noexcept { return m_abortQueued.load(); }
#endif

    Executor& GetExecutor() noexcept {
        return m_executor;
    }

    template <class Function>
    bool Post(Function&& p_function, std::size_t p_accountedBytes = 0, bool p_critical = false) {
        std::lock_guard lock(m_stateMutex);
        if (m_stopping) {
            return false;
        }
        std::shared_ptr<const SocketResourceBudget::Reservation> reservation;
        if (p_critical) {
            if (m_queuedCritical >= kMaxCriticalTasks) return false;
            ++m_queuedCritical;
        } else {
            if (m_queuedTasks >= kMaxQueuedTasks ||
                p_accountedBytes > kMaxQueuedBytes - m_queuedBytes) return false;
            reservation = SocketResourceBudget::Reserve(p_accountedBytes);
            if (!reservation) return false;
            ++m_queuedTasks;
            m_queuedBytes += p_accountedBytes;
        }
        try {
#if defined(DEEPBIDE_NATIVE_TEST)
            if (m_failPost.exchange(false)) throw std::bad_alloc();
#endif
            asio::post(m_executor, [this, task = std::forward<Function>(p_function), p_accountedBytes, p_critical, reservation = std::move(reservation)]() mutable {
                try {
                    // If finalizer submission failed, an external retirement
                    // drains only canceled completions, never queued commands.
                    if (!m_abortQueued.load(std::memory_order_acquire)) task();
                } catch (...) {
                    ReleasePost(p_accountedBytes, p_critical);
                    throw;
                }
                ReleasePost(p_accountedBytes, p_critical);
            });
        } catch (...) {
            ReleasePostLocked(p_accountedBytes, p_critical);
            return false;
        }
        return true;
    }

    template <class Finalizer>
    bool Stop(Finalizer&& p_finalizer,
              std::chrono::milliseconds p_diagnosticWait = std::chrono::seconds(5)) {
        {
            std::lock_guard lock(m_stateMutex);
            if (!m_stopping) {
                m_stopping = true;
                try {
#if defined(DEEPBIDE_NATIVE_TEST)
                    if (m_failFinalizer.exchange(false)) throw std::bad_alloc();
#endif
                    asio::post(m_executor, [this, finalizer = std::forward<Finalizer>(p_finalizer)]() mutable {
                        try {
                            finalizer();
                        } catch (const std::exception&) {
                            m_workerFailed.store(true, std::memory_order_release);
                        } catch (...) {
                            m_workerFailed.store(true, std::memory_order_release);
                        }
                        // Stop accepting commands before this point. The owner
                        // finalizer cancels its I/O; let Asio deliver all
                        // cancellation completions before run() exits.
                        m_work.reset();
                    });
                } catch (...) {
                    m_workerFailed.store(true, std::memory_order_release);
                    m_abortQueued.store(true, std::memory_order_release);
                    m_context.stop();
                }
            }
        }

        {
            std::lock_guard joinLock(m_joinMutex);
            if (m_worker.joinable() && m_worker.get_id() == std::this_thread::get_id()) {
                // Do not stop before the queued finalizer runs. The worker
                // cannot join itself; an external owner must call Stop again
                // (or destroy this object) to reap the joinable thread.
                return false;
            }
        }
        if (m_workerExited.wait_for(p_diagnosticWait) != std::future_status::ready) {
            // A timeout is diagnostic only. Do not stop the context or destroy
            // queued cancellation completions: return with the worker
            // joinable so a later external Stop can reap it safely.
            return false;
        }
        std::lock_guard joinLock(m_joinMutex);
        if (m_worker.joinable() && m_worker.get_id() != std::this_thread::get_id()) {
            m_worker.join();
        }
        return !m_workerFailed.load(std::memory_order_acquire);
    }

    // After Stop has requested finalization, external lifetime owners must
    // wait for actual worker exit before releasing any operation state.
    // This is intentionally not a hard shutdown deadline.
    bool ReapStoppedWorker() {
        if (IsWorkerThread()) return false;
        m_workerExited.wait();
        std::lock_guard joinLock(m_joinMutex);
        if (m_worker.joinable()) m_worker.join();
        return true;
    }

    // Exceptional submission failure: the lifetime owner must first join
    // the worker and invalidate/close all transport epochs. Only then can it
    // drain retained cancellation handlers without an active I/O owner.
    // Queued commands are skipped; callbacks must already be revoked.
    // As with normal Stop, there is no hard shutdown deadline.
    bool DrainAbortedAfterJoin() {
        if (!m_abortQueued.load(std::memory_order_acquire)) return true;
        if (!ReapStoppedWorker()) return false;
        std::lock_guard drain(m_abortDrainMutex);
        if (m_abortDrained) return true;
        m_work.reset();
        m_context.restart();
        for (;;) {
            try { m_context.run(); break; }
            catch (...) { m_workerFailed.store(true, std::memory_order_release); }
        }
        m_abortDrained = true;
        return true;
    }

private:
    bool IsWorkerThread() {
        std::lock_guard joinLock(m_joinMutex);
        return m_worker.joinable() && m_worker.get_id() == std::this_thread::get_id();
    }

    void ReleasePost(std::size_t p_bytes, bool p_critical) noexcept {
        std::lock_guard lock(m_stateMutex);
        ReleasePostLocked(p_bytes, p_critical);
    }

    void ReleasePostLocked(std::size_t p_bytes, bool p_critical) noexcept {
        if (p_critical) {
            if (m_queuedCritical) --m_queuedCritical;
        } else {
            if (m_queuedTasks) --m_queuedTasks;
            m_queuedBytes = p_bytes <= m_queuedBytes ? m_queuedBytes - p_bytes : 0;
        }
    }

    asio::io_context m_context;
    Executor m_executor;
    asio::executor_work_guard<asio::io_context::executor_type> m_work;
    std::mutex m_stateMutex;
    std::mutex m_joinMutex;
    std::mutex m_abortDrainMutex;
    std::atomic_bool m_abortQueued{false};
    bool m_abortDrained{false};
#if defined(DEEPBIDE_NATIVE_TEST)
    std::atomic_bool m_failPost{false}, m_failFinalizer{false};
#endif
    std::atomic_bool m_workerFailed{false};
    bool m_stopping{false};
    std::size_t m_queuedTasks{0};
    std::size_t m_queuedBytes{0};
    std::size_t m_queuedCritical{0};
    std::promise<void> m_workerExitedPromise;
    std::shared_future<void> m_workerExited;
    std::thread m_worker;
};
