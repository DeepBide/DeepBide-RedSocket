#include "DeferredHandleRegistry.h"
#include "SocketSessionGate.h"
#include "SocketUpdateQuiescence.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <future>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {
void Check(bool p_condition, const char* p_expression, int p_line) {
    if (p_condition) return;
    std::cerr << "CHECK failed at line " << p_line << ": " << p_expression << '\n';
    std::exit(2);
}
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)
struct ReleaseProbe {
    uint64_t id{0};
    std::atomic<uint64_t> releases{0};
    std::thread::id finalReleaseThread{};
};

class TrackedHandle {
public:
    TrackedHandle() = default;
    TrackedHandle(std::shared_ptr<ReleaseProbe> p_probe, uint64_t p_id) : m_probe(std::move(p_probe)) {
        m_probe->id = p_id;
    }
    TrackedHandle(const TrackedHandle&) = delete;
    TrackedHandle& operator=(const TrackedHandle&) = delete;
    TrackedHandle(TrackedHandle&&) noexcept = default;
    TrackedHandle& operator=(TrackedHandle&& p_other) noexcept {
        Reset();
        m_probe = std::move(p_other.m_probe);
        return *this;
    }
    ~TrackedHandle() { Reset(); }
    explicit operator bool() const noexcept { return static_cast<bool>(m_probe); }

private:
    void Reset() noexcept {
        if (!m_probe) return;
        m_probe->finalReleaseThread = std::this_thread::get_id();
        m_probe->releases.fetch_add(1, std::memory_order_relaxed);
        m_probe.reset();
    }
    std::shared_ptr<ReleaseProbe> m_probe;
};
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--probe-check-failure") CHECK(false);
#ifdef NDEBUG
    std::cout << "HANDLE_LIFETIME_CHECKS_ALWAYS_ON build=NDEBUG\n";
#else
    std::cout << "HANDLE_LIFETIME_CHECKS_ALWAYS_ON build=debug\n";
#endif
    using Registry = DeferredHandleRegistry<TrackedHandle, uint32_t>;
    Registry registry;
    const auto pumpThread = std::this_thread::get_id();
    auto probe = std::make_shared<ReleaseProbe>();
    const bool inserted17 = registry.Insert(17, TrackedHandle{probe, 17}, 5, 2);
    CHECK(inserted17);
    auto duplicate = std::make_shared<ReleaseProbe>();
    const bool duplicateInserted = registry.Insert(17, TrackedHandle{duplicate, 170}, 6, 2);
    CHECK(!duplicateInserted); // duplicate ID

    std::thread worker([&] {
        const bool marked = registry.RequestClear(17);
        CHECK(marked);
        const bool staleMarked = registry.RequestClear(999);
        CHECK(!staleMarked); // stale ID is harmless
    });
    worker.join();

    // The worker only marks the record. Last release waits for the pump drain.
    CHECK(probe->releases.load(std::memory_order_relaxed) == 0);
    auto cleared = registry.DrainCleared();
    CHECK(cleared.size() == 1);
    cleared.clear();
    CHECK(probe->releases.load(std::memory_order_relaxed) == 1);
    CHECK(probe->id == 17);
    CHECK(probe->finalReleaseThread == pumpThread);
    CHECK(registry.Size() == 0);

    auto second = std::make_shared<ReleaseProbe>();
    const bool inserted18 = registry.Insert(18, TrackedHandle{second, 18}, 9, 1);
    CHECK(inserted18);
    auto rejected = std::make_shared<ReleaseProbe>();
    const bool inserted19 = registry.Insert(19, TrackedHandle{rejected, 190}, 10, 1);
    CHECK(!inserted19); // bounded registry
    Registry::Entry taken;
    const bool took18 = registry.Take(18, taken);
    CHECK(took18);
    Registry::Entry stale;
    const bool tookStale = registry.Take(18, stale);
    CHECK(!tookStale);
    taken = Registry::Entry{};
    CHECK(second->releases.load(std::memory_order_relaxed) == 1);
    CHECK(second->finalReleaseThread == pumpThread);

    auto shutdown = std::make_shared<ReleaseProbe>();
    const bool inserted20 = registry.Insert(20, TrackedHandle{shutdown, 20}, 11, 1);
    CHECK(inserted20);
    auto finalEntries = registry.DrainAll(); // STOP/Unload lifecycle drain
    CHECK(finalEntries.size() == 1);
    finalEntries.clear();
    CHECK(shutdown->releases.load(std::memory_order_relaxed) == 1);
    CHECK(shutdown->id == 20);
    CHECK(shutdown->finalReleaseThread == pumpThread);

    SocketUpdateQuiescence quiescence;
    std::promise<void> updateEntered;
    std::promise<void> allowUpdateExit;
    auto allowExit = allowUpdateExit.get_future();
    std::thread update([&] {
        auto lease = quiescence.TryEnter();
        CHECK(static_cast<bool>(lease));
        updateEntered.set_value();
        allowExit.wait();
    });
    updateEntered.get_future().wait();
    std::promise<void> unloadFinished;
    std::thread unload([&] {
        const bool quiesced = quiescence.CloseAndWait();
        CHECK(quiesced);
        unloadFinished.set_value();
    });
    CHECK(unloadFinished.get_future().wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    allowUpdateExit.set_value();
    update.join();
    unload.join();
    CHECK(!quiescence.TryEnter());

    SocketUpdateQuiescence reentrant;
    auto active = reentrant.TryEnter();
    CHECK(static_cast<bool>(active));
    CHECK(!reentrant.CloseAndWait()); // fail-closed signal, no self-deadlock
    active.Release();
    const bool reentrantQuiesced = reentrant.CloseAndWait();
    CHECK(reentrantQuiesced);
    CHECK(reentrant.Reopen());
    CHECK(static_cast<bool>(reentrant.TryEnter()));

    SocketUpdateQuiescence cleanupGate;
    auto cleanupLease = cleanupGate.TryEnter();
    unsigned cleanupCalls = 0;
    const bool cleanupRanReentrantly = cleanupGate.CloseAndRunIfQuiescent([&] { ++cleanupCalls; });
    CHECK(!cleanupRanReentrantly && cleanupCalls == 0);
    cleanupLease.Release();
    const bool cleanupRanAfterLease = cleanupGate.CloseAndRunIfQuiescent([&] { ++cleanupCalls; });
    CHECK(cleanupRanAfterLease && cleanupCalls == 1);

    SocketUpdateQuiescence concurrentCleanupGate;
    std::promise<void> firstEntered;
    std::promise<void> requestReentrantStop;
    std::promise<void> stopDeferred;
    std::promise<void> secondEntered;
    std::promise<void> allowSecondExit;
    auto stopRequest = requestReentrantStop.get_future();
    auto allowSecond = allowSecondExit.get_future();
    std::atomic<unsigned> concurrentCleanupCalls{0};
    std::thread firstUpdate([&] {
        auto firstLease = concurrentCleanupGate.TryEnter();
        CHECK(static_cast<bool>(firstLease));
        firstEntered.set_value();
        stopRequest.wait();
        const bool stoppedReentrantly = concurrentCleanupGate.CloseAndRunIfQuiescent([] {});
        CHECK(!stoppedReentrantly);
        stopDeferred.set_value();
        const bool cleaned = concurrentCleanupGate.ReleaseAndCloseAndRunIfQuiescent(
            firstLease, [&] { concurrentCleanupCalls.fetch_add(1, std::memory_order_relaxed); });
        CHECK(cleaned);
    });
    firstEntered.get_future().wait();
    std::thread secondUpdate([&] {
        auto secondLease = concurrentCleanupGate.TryEnter();
        CHECK(static_cast<bool>(secondLease));
        secondEntered.set_value();
        allowSecond.wait();
    });
    secondEntered.get_future().wait();
    requestReentrantStop.set_value();
    stopDeferred.get_future().wait();
    for (unsigned attempt = 0; attempt < 100 && concurrentCleanupGate.Active() != 1; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(concurrentCleanupGate.Active() == 1);
    CHECK(concurrentCleanupCalls.load(std::memory_order_relaxed) == 0);
    allowSecondExit.set_value();
    firstUpdate.join();
    secondUpdate.join();
    CHECK(concurrentCleanupCalls.load(std::memory_order_relaxed) == 1);

    // Reopen and admission are both denied for the entire cleanup interval,
    // including the period after the active update count reached zero.
    SocketUpdateQuiescence exclusiveCleanupGate;
    std::promise<void> exclusiveCleanupEntered;
    std::promise<void> allowExclusiveCleanupExit;
    auto allowExclusiveExit = allowExclusiveCleanupExit.get_future();
    std::thread exclusiveCleaner([&] {
        const bool cleaned = exclusiveCleanupGate.CloseAndRunIfQuiescent([&] {
            exclusiveCleanupEntered.set_value();
            allowExclusiveExit.wait();
        });
        CHECK(cleaned);
    });
    exclusiveCleanupEntered.get_future().wait();
    CHECK(exclusiveCleanupGate.Active() == 0);
    CHECK(exclusiveCleanupGate.PendingCleanups() == 1);
    CHECK(!exclusiveCleanupGate.Reopen());
    CHECK(!exclusiveCleanupGate.TryEnter());
    allowExclusiveCleanupExit.set_value();
    exclusiveCleaner.join();
    CHECK(exclusiveCleanupGate.PendingCleanups() == 0);
    CHECK(exclusiveCleanupGate.Reopen());
    auto afterCleanupLease = exclusiveCleanupGate.TryEnter();
    CHECK(static_cast<bool>(afterCleanupLease));
    afterCleanupLease.Release();

    // A Stop requested from Update defers cleanup; a concurrent Start must
    // wait for that cleanup, then retain resources created for the new epoch.
    SocketUpdateQuiescence restartGate;
    SocketSessionGate restartSession;
    std::mutex restartSessionMutex;
    std::mutex restartResourcesMutex;
    std::vector<int> restartResources{101};
    std::unordered_map<int, unsigned> restartDestroyed;
    std::atomic<bool> restartStopPending{false};
    std::promise<void> restartUpdateEntered;
    std::promise<void> requestRestartStop;
    std::promise<void> restartStopDeferred;
    std::promise<void> allowRestartUpdateExit;
    std::promise<void> restartCleanupEntered;
    std::promise<void> allowRestartCleanupExit;
    std::promise<void> restartAttempting;
    std::promise<void> restartFinished;
    auto restartStopRequest = requestRestartStop.get_future();
    auto allowRestartExit = allowRestartUpdateExit.get_future();
    auto allowRestartCleanup = allowRestartCleanupExit.get_future();
    auto performRestartCleanup = [&] {
        std::vector<int> oldResources;
        {
            std::lock_guard lock(restartResourcesMutex);
            oldResources.swap(restartResources);
        }
        restartCleanupEntered.set_value();
        allowRestartCleanup.wait();
        std::lock_guard lock(restartResourcesMutex);
        for (const auto resource : oldResources) ++restartDestroyed[resource];
    };
    {
        std::lock_guard lock(restartSessionMutex);
        CHECK(restartSession.StartSession());
    }
    std::thread restartUpdate([&] {
        auto lease = restartGate.TryEnter();
        CHECK(static_cast<bool>(lease));
        restartUpdateEntered.set_value();
        restartStopRequest.wait();
        const bool stoppedReentrantly = restartGate.CloseAndRunIfQuiescent([&] {
            std::lock_guard lock(restartSessionMutex);
            restartSession.StopSession();
        });
        CHECK(!stoppedReentrantly);
        {
            std::lock_guard lock(restartSessionMutex);
            restartSession.StopSession();
        }
        restartStopPending.store(true, std::memory_order_release);
        restartStopDeferred.set_value();
        allowRestartExit.wait();
        if (restartStopPending.load(std::memory_order_acquire)) {
            const bool cleaned = restartGate.ReleaseAndCloseAndRunIfQuiescent(lease, [&] {
                if (restartStopPending.exchange(false, std::memory_order_acq_rel)) {
                    performRestartCleanup();
                }
            });
            CHECK(cleaned);
        } else {
            lease.Release();
        }
    });
    restartUpdateEntered.get_future().wait();
    requestRestartStop.set_value();
    restartStopDeferred.get_future().wait();
    std::thread restartStart([&] {
        restartAttempting.set_value();
        const bool transitioned = restartGate.CloseAndRunIfQuiescent([&] {
            if (restartStopPending.exchange(false, std::memory_order_acq_rel)) {
                performRestartCleanup();
            }
            std::lock_guard sessionLock(restartSessionMutex);
            CHECK(restartSession.StartSession());
            std::lock_guard resourcesLock(restartResourcesMutex);
            restartResources.push_back(202);
        }, true);
        CHECK(transitioned);
        restartFinished.set_value();
    });
    restartAttempting.get_future().wait();
    allowRestartUpdateExit.set_value();
    restartCleanupEntered.get_future().wait();
    CHECK(!restartGate.TryEnter());
    CHECK(restartFinished.get_future().wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    {
        std::lock_guard lock(restartSessionMutex);
        CHECK(!restartSession.Accepting());
    }
    allowRestartCleanupExit.set_value();
    restartUpdate.join();
    restartStart.join();
    {
        std::lock_guard lock(restartSessionMutex);
        CHECK(restartSession.Accepting());
    }
    {
        std::lock_guard lock(restartResourcesMutex);
        CHECK(restartResources.size() == 1 && restartResources.front() == 202);
        CHECK(restartDestroyed[101] == 1);
        CHECK(restartDestroyed[202] == 0);
    }
    auto restartedLease = restartGate.TryEnter();
    CHECK(static_cast<bool>(restartedLease));
    restartedLease.Release();

    // Stop and Unload cleanup requests serialize. The second caller remains
    // queued while the first cleanup is blocked; no resource is destroyed twice.
    SocketUpdateQuiescence overlapGate;
    SocketSessionGate overlapSession;
    std::mutex overlapSessionMutex;
    std::mutex overlapResourcesMutex;
    std::vector<int> overlapResources{303};
    std::unordered_map<int, unsigned> overlapDestroyed;
    std::atomic<unsigned> activeCleanups{0};
    std::atomic<unsigned> maxConcurrentCleanups{0};
    std::atomic<unsigned> stopCleanupCalls{0};
    std::atomic<unsigned> unloadCleanupCalls{0};
    std::promise<void> stopCleanupEntered;
    std::promise<void> allowStopCleanupExit;
    std::promise<void> unloadRequested;
    std::promise<void> q1UnloadFinished;
    auto allowStopCleanup = allowStopCleanupExit.get_future();
    std::thread overlappingStop([&] {
        const bool stopped = overlapGate.CloseAndRunIfQuiescent([&] {
            {
                std::lock_guard lock(overlapSessionMutex);
                overlapSession.StopSession();
            }
            stopCleanupCalls.fetch_add(1, std::memory_order_relaxed);
            const auto now = activeCleanups.fetch_add(1, std::memory_order_acq_rel) + 1;
            auto maximum = maxConcurrentCleanups.load(std::memory_order_relaxed);
            while (maximum < now && !maxConcurrentCleanups.compare_exchange_weak(
                       maximum, now, std::memory_order_relaxed)) {}
            std::vector<int> oldResources;
            {
                std::lock_guard lock(overlapResourcesMutex);
                oldResources.swap(overlapResources);
            }
            stopCleanupEntered.set_value();
            allowStopCleanup.wait();
            {
                std::lock_guard lock(overlapResourcesMutex);
                for (const auto resource : oldResources) ++overlapDestroyed[resource];
            }
            activeCleanups.fetch_sub(1, std::memory_order_acq_rel);
        });
        CHECK(stopped);
    });
    stopCleanupEntered.get_future().wait();
    std::thread overlappingUnload([&] {
        unloadRequested.set_value();
        const bool unloaded = overlapGate.CloseAndRunIfQuiescent([&] {
            {
                std::lock_guard lock(overlapSessionMutex);
                overlapSession.Unload();
            }
            unloadCleanupCalls.fetch_add(1, std::memory_order_relaxed);
            const auto now = activeCleanups.fetch_add(1, std::memory_order_acq_rel) + 1;
            auto maximum = maxConcurrentCleanups.load(std::memory_order_relaxed);
            while (maximum < now && !maxConcurrentCleanups.compare_exchange_weak(
                       maximum, now, std::memory_order_relaxed)) {}
            std::vector<int> oldResources;
            {
                std::lock_guard lock(overlapResourcesMutex);
                oldResources.swap(overlapResources);
            }
            {
                std::lock_guard lock(overlapResourcesMutex);
                for (const auto resource : oldResources) ++overlapDestroyed[resource];
            }
            activeCleanups.fetch_sub(1, std::memory_order_acq_rel);
        });
        CHECK(unloaded);
        q1UnloadFinished.set_value();
    });
    unloadRequested.get_future().wait();
    for (unsigned attempt = 0; attempt < 100 && overlapGate.PendingCleanups() < 2; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(overlapGate.PendingCleanups() == 2);
    CHECK(!overlapGate.Reopen());
    CHECK(!overlapGate.TryEnter());
    CHECK(maxConcurrentCleanups.load(std::memory_order_relaxed) == 1);
    allowStopCleanupExit.set_value();
    overlappingStop.join();
    overlappingUnload.join();
    CHECK(q1UnloadFinished.get_future().wait_for(std::chrono::milliseconds(0)) == std::future_status::ready);
    CHECK(stopCleanupCalls.load(std::memory_order_relaxed) == 1);
    CHECK(unloadCleanupCalls.load(std::memory_order_relaxed) == 1);
    CHECK(maxConcurrentCleanups.load(std::memory_order_relaxed) == 1);
    CHECK(activeCleanups.load(std::memory_order_relaxed) == 0);
    {
        std::lock_guard lock(overlapResourcesMutex);
        CHECK(overlapResources.empty());
        CHECK(overlapDestroyed[303] == 1);
    }
    {
        std::lock_guard lock(overlapSessionMutex);
        CHECK(overlapSession.IsUnloaded());
        CHECK(!overlapSession.Accepting());
    }
    return 0;
}
