#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <type_traits>
#include <utility>

// Tracks plugin code currently executing through the Running update pump.
// Unload closes admission and waits for external in-flight pumps to return.
// Reentrant unload is reported instead of self-deadlocking.
class SocketUpdateQuiescence {
public:
    class Lease {
    public:
        Lease() = default;
        explicit Lease(SocketUpdateQuiescence* p_owner) : m_owner(p_owner), m_previous(s_current) {
            s_current = p_owner;
        }
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& p_other) noexcept : m_owner(p_other.m_owner), m_previous(p_other.m_previous) {
            p_other.m_owner = nullptr;
        }
        ~Lease() { Release(); }
        explicit operator bool() const noexcept { return m_owner != nullptr; }
        void Release() noexcept {
            if (!m_owner) return;
            s_current = m_previous;
            m_owner->Leave();
            m_owner = nullptr;
        }
    private:
        SocketUpdateQuiescence* m_owner{nullptr};
        SocketUpdateQuiescence* m_previous{nullptr};
    };

    Lease TryEnter() {
        std::lock_guard lock(m_mutex);
        if (m_closed || m_cleanupInProgress) return {};
        ++m_active;
        return Lease(this);
    }

    // Native dispatch has one invocation context. Reentrant or overlapping
    // updates do not consume another event while the first callback is live.
    Lease TryEnterExclusive() {
        std::lock_guard lock(m_mutex);
        if (m_closed || m_cleanupInProgress || m_active != 0) return {};
        ++m_active;
        return Lease(this);
    }

    bool CloseAndWait() {
        std::unique_lock lock(m_mutex);
        m_closed = true;
        ++m_closeEpoch;
        if (s_current == this || s_cleanupCurrent == this) return false;
        m_idle.wait(lock, [this] { return m_active == 0 && !m_cleanupInProgress; });
        return true;
    }

    bool Reopen() {
        std::lock_guard lock(m_mutex);
        if (m_active != 0 || m_cleanupInProgress || m_pendingCleanups != 0) return false;
        m_closed = false;
        return true;
    }

    void Close() {
        std::lock_guard lock(m_mutex);
        m_closed = true;
        ++m_closeEpoch;
    }

    bool Closed() const {
        std::lock_guard lock(m_mutex);
        return m_closed;
    }

    template<class Cleanup>
    bool CloseAndRunIfQuiescent(Cleanup&& p_cleanup, bool p_reopenAfter = false,
                                bool p_newCloseIntent = true) {
        std::unique_lock lock(m_mutex);
        m_closed = true;
        const auto closeEpoch = p_newCloseIntent ? ++m_closeEpoch : m_closeEpoch;
        ++m_pendingCleanups;
        if (s_current == this || s_cleanupCurrent == this) {
            --m_pendingCleanups;
            m_idle.notify_all();
            return false;
        }
        m_idle.wait(lock, [this] { return m_active == 0 && !m_cleanupInProgress; });
        m_cleanupInProgress = true;
        auto* previousCleanup = s_cleanupCurrent;
        s_cleanupCurrent = this;
        lock.unlock();

        bool reopenAfterCleanup = p_reopenAfter;
        try {
            if constexpr (std::is_same_v<std::invoke_result_t<Cleanup>, bool>) {
                reopenAfterCleanup = std::forward<Cleanup>(p_cleanup)();
            } else {
                std::forward<Cleanup>(p_cleanup)();
            }
        } catch (...) {
            lock.lock();
            s_cleanupCurrent = previousCleanup;
            m_cleanupInProgress = false;
            --m_pendingCleanups;
            m_idle.notify_all();
            lock.unlock();
            throw;
        }

        lock.lock();
        s_cleanupCurrent = previousCleanup;
        m_cleanupInProgress = false;
        --m_pendingCleanups;
        if (reopenAfterCleanup && closeEpoch == m_closeEpoch && m_pendingCleanups == 0) {
            m_closed = false;
        }
        m_idle.notify_all();
        return true;
    }

    template<class Cleanup>
    bool ReleaseAndCloseAndRunIfQuiescent(Lease& p_lease, Cleanup&& p_cleanup) {
        p_lease.Release();
        return CloseAndRunIfQuiescent(std::forward<Cleanup>(p_cleanup), false, false);
    }

    std::size_t Active() const {
        std::lock_guard lock(m_mutex);
        return m_active;
    }

    std::size_t PendingCleanups() const {
        std::lock_guard lock(m_mutex);
        return m_pendingCleanups;
    }

private:
    void Leave() noexcept {
        std::lock_guard lock(m_mutex);
        if (--m_active == 0) m_idle.notify_all();
    }

    mutable std::mutex m_mutex;
    std::condition_variable m_idle;
    std::size_t m_active{0};
    bool m_closed{false};
    bool m_cleanupInProgress{false};
    std::size_t m_pendingCleanups{0};
    uint64_t m_closeEpoch{0};
    inline static thread_local SocketUpdateQuiescence* s_current{nullptr};
    inline static thread_local SocketUpdateQuiescence* s_cleanupCurrent{nullptr};
};
