#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <utility>

// Cooperative per-Update work slice. The time limit is checked between event
// callbacks and is not a frame-time SLA: one callback cannot be preempted.
class SocketEventDispatchBudget {
public:
    static constexpr std::size_t kMaxEventsPerUpdate = 64;
    static constexpr std::size_t kMaxBytesPerUpdate = 1024 * 1024 + 512;
    static constexpr std::chrono::milliseconds kSoftTimePerUpdate{2};

    template <class Rep, class Period>
    static constexpr bool CanDispatchNext(
        std::size_t p_dispatchedEvents,
        std::size_t p_dispatchedBytes,
        std::size_t p_nextEventBytes,
        std::chrono::duration<Rep, Period> p_elapsed) noexcept {
        if (p_dispatchedEvents >= kMaxEventsPerUpdate || p_nextEventBytes > kMaxBytesPerUpdate)
            return false;
        if (p_dispatchedEvents == 0)
            return true; // guarantee progress when Update is called
        if (p_dispatchedBytes > kMaxBytesPerUpdate ||
            p_nextEventBytes > kMaxBytesPerUpdate - p_dispatchedBytes)
            return false;
        return p_elapsed < kSoftTimePerUpdate;
    }
};

// Selects from the bounded pending queue without violating per-socket causal
// order. Cross-socket total order is not a public callback contract.
class SocketEventDispatchScheduler {
public:
    struct SelectionDiagnostics {
        std::size_t EventsScanned{0};
        std::size_t IdentityProbeSteps{0};
        bool CapacityExceeded{false};
    };

    static constexpr std::size_t kMaxConsecutiveCritical = 16;
    static constexpr std::size_t kMaxSocketIdentities = 128;
    static constexpr std::size_t kMaxCriticalIdentities = 256;
    static constexpr std::size_t kMaxDistinctPendingIdentities =
        kMaxSocketIdentities + kMaxCriticalIdentities;
    static constexpr std::size_t kMaxPendingEvents = 4096 + kMaxCriticalIdentities;
    static constexpr std::size_t kIdentitySetSlots = 512;
    static_assert((kIdentitySetSlots & (kIdentitySetSlots - 1)) == 0);
    static_assert(kIdentitySetSlots > kMaxDistinctPendingIdentities);

    template <class EventQueue, class Rep, class Period>
    static auto SelectNext(
        EventQueue& p_events,
        std::size_t p_dispatchedEvents,
        std::size_t p_dispatchedBytes,
        std::chrono::duration<Rep, Period> p_elapsed,
        std::size_t p_criticalStreak,
        SelectionDiagnostics* p_diagnostics = nullptr) noexcept -> typename EventQueue::iterator {
        if (p_diagnostics) *p_diagnostics = {};
        if (p_events.size() > kMaxPendingEvents) {
            if (p_diagnostics) p_diagnostics->CapacityExceeded = true;
            return p_events.end();
        }

        // All candidates are ineligible once the count/time slice expires.
        if (!SocketEventDispatchBudget::CanDispatchNext(p_dispatchedEvents, p_dispatchedBytes, 0, p_elapsed))
            return p_events.end();

        auto selectedCritical = p_events.end();
        auto selectedNormal = p_events.end();
        std::array<uint64_t, kIdentitySetSlots> seenIdentities{};
        std::array<bool, kIdentitySetSlots> occupied{};

        for (auto event = p_events.begin(); event != p_events.end(); ++event) {
            if (p_diagnostics) ++p_diagnostics->EventsScanned;
            auto slot = static_cast<std::size_t>(
                (event->Identity ^ (event->Identity >> 32)) & (kIdentitySetSlots - 1));
            bool identityHandled = false;
            bool alreadySeen = false;
            for (std::size_t probe = 0; probe < kIdentitySetSlots; ++probe) {
                if (p_diagnostics) ++p_diagnostics->IdentityProbeSteps;
                if (!occupied[slot]) {
                    occupied[slot] = true;
                    seenIdentities[slot] = event->Identity;
                    identityHandled = true;
                    break;
                }
                if (seenIdentities[slot] == event->Identity) {
                    identityHandled = true;
                    alreadySeen = true;
                    break;
                }
                slot = (slot + 1) & (kIdentitySetSlots - 1);
            }
            if (!identityHandled) {
                if (p_diagnostics) p_diagnostics->CapacityExceeded = true;
                return p_events.end();
            }
            if (alreadySeen) continue;

            if (!SocketEventDispatchBudget::CanDispatchNext(
                    p_dispatchedEvents, p_dispatchedBytes, event->AccountedBytes, p_elapsed))
                continue;
            if (event->Critical) {
                if (selectedCritical == p_events.end()) selectedCritical = event;
            } else if (selectedNormal == p_events.end()) {
                selectedNormal = event;
            }
            if (selectedCritical != p_events.end() && selectedNormal != p_events.end()) break;
        }

        if (selectedCritical != p_events.end() &&
            p_criticalStreak < kMaxConsecutiveCritical)
            return selectedCritical;
        if (selectedNormal != p_events.end()) return selectedNormal;
        return selectedCritical;
    }
};

// Accounting limits for the game-thread event pump. Per-socket quotas ensure
// one noisy connection cannot consume the entire normal lane. Critical
// lifecycle/error events use a separately bounded reserve.
class SocketEventBudget {
public:
    static constexpr std::size_t kMaxSockets = 128;
    static constexpr std::size_t kMaxEvents = 4096;
    static constexpr std::size_t kMaxBytes = 8 * 1024 * 1024;
    static constexpr std::size_t kMaxEventsPerSocket = 2048;
    static constexpr std::size_t kMaxBytesPerSocket = 4 * 1024 * 1024;
    static constexpr std::size_t kMaxCriticalPerSocket = 2;
    static constexpr std::size_t kMaxCriticalEvents = kMaxSockets * kMaxCriticalPerSocket;

    bool TryReserve(uint64_t p_socketIdentity, std::size_t p_bytes, bool p_critical) {
        if (p_bytes > SocketEventDispatchBudget::kMaxBytesPerUpdate) return false;
        if (p_critical) {
            if (!p_socketIdentity || m_criticalEvents >= kMaxCriticalEvents) return false;
            auto it = m_criticalPerSocket.find(p_socketIdentity);
            if (it == m_criticalPerSocket.end()) {
                if (m_criticalPerSocket.size() >= kMaxSockets) return false;
                it = m_criticalPerSocket.emplace(p_socketIdentity, 0).first;
            }
            if (it->second >= kMaxCriticalPerSocket) return false;
            ++it->second;
            ++m_criticalEvents;
            return true;
        }
        if (p_socketIdentity == 0 || m_events >= kMaxEvents || p_bytes > kMaxBytes - m_bytes)
            return false;
        auto it = m_perSocket.find(p_socketIdentity);
        if (it == m_perSocket.end()) {
            if (m_perSocket.size() >= kMaxSockets) return false;
            it = m_perSocket.emplace(p_socketIdentity, Usage{}).first;
        }
        if (it->second.events >= kMaxEventsPerSocket || p_bytes > kMaxBytesPerSocket - it->second.bytes)
            return false;
        ++m_events;
        m_bytes += p_bytes;
        ++it->second.events;
        it->second.bytes += p_bytes;
        return true;
    }

    void Release(uint64_t p_socketIdentity, std::size_t p_bytes, bool p_critical) noexcept {
        if (p_critical) {
            const auto it = m_criticalPerSocket.find(p_socketIdentity);
            if (it == m_criticalPerSocket.end()) return;
            --m_criticalEvents;
            if (--it->second == 0) m_criticalPerSocket.erase(it);
            return;
        }
        if (m_events > 0) --m_events;
        m_bytes = p_bytes <= m_bytes ? m_bytes - p_bytes : 0;
        const auto it = m_perSocket.find(p_socketIdentity);
        if (it == m_perSocket.end()) return;
        if (it->second.events > 0) --it->second.events;
        it->second.bytes = p_bytes <= it->second.bytes ? it->second.bytes - p_bytes : 0;
        if (it->second.events == 0) m_perSocket.erase(it);
    }

    void Reset() noexcept {
        m_events = m_bytes = m_criticalEvents = 0;
        m_perSocket.clear();
        m_criticalPerSocket.clear();
    }

    // Queue reservation and allocation are one transaction. A failed push
    // cannot strand a charged slot, even under allocation failure.
    template<class Push>
    bool TryEnqueue(uint64_t identity, std::size_t bytes, bool critical, Push&& push) noexcept {
        try {
            if (!TryReserve(identity, bytes, critical)) return false;
            try {
                std::forward<Push>(push)();
            } catch (...) {
                Release(identity, bytes, critical);
                return false;
            }
            return true;
        } catch (...) { return false; }
    }

    template<class Queue>
    void PurgeIdentity(Queue& queue, uint64_t identity) noexcept {
        for (auto it = queue.begin(); it != queue.end();) {
            if (it->Identity != identity) { ++it; continue; }
            Release(it->Identity, it->AccountedBytes, it->Critical);
            it = queue.erase(it);
        }
    }

    std::size_t EventCount() const noexcept { return m_events; }
    std::size_t ByteCount() const noexcept { return m_bytes; }
    std::size_t CriticalCount() const noexcept { return m_criticalEvents; }

private:
    struct Usage { std::size_t events{0}; std::size_t bytes{0}; };
    std::size_t m_events{0};
    std::size_t m_bytes{0};
    std::size_t m_criticalEvents{0};
    std::unordered_map<uint64_t, Usage> m_perSocket;
    std::unordered_map<uint64_t, std::size_t> m_criticalPerSocket;
};

static_assert(SocketEventDispatchScheduler::kMaxSocketIdentities == SocketEventBudget::kMaxSockets);
static_assert(SocketEventDispatchScheduler::kMaxCriticalIdentities == SocketEventBudget::kMaxCriticalEvents);
static_assert(SocketEventDispatchScheduler::kMaxPendingEvents ==
    SocketEventBudget::kMaxEvents + SocketEventBudget::kMaxCriticalEvents);
