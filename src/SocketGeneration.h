#pragma once

#include <cstdint>

enum class SocketLifecycleState : uint8_t {
    Disconnected,
    Resolving,
    Connecting,
    Connected,
    Closing,
    Destroyed,
};

struct SocketClosePlan {
    bool Apply{false};
    bool NotifyDisconnection{false};
};

// Generation zero is reserved for an operator-requested close of the current
// connection. Error-driven closes must pass the generation they observed.
constexpr bool IsCurrentSocketGeneration(uint64_t p_expected, uint64_t p_current) noexcept {
    return p_expected == 0 || p_expected == p_current;
}

// Production close gate shared by RedSocket::CloseOnExecutor and the native
// regression harness. A disconnected socket can still be destroyed, but a
// redundant Disconnect must neither touch the transport nor advance generation.
constexpr SocketClosePlan PlanSocketClose(uint64_t p_expectedGeneration,
                                          uint64_t p_currentGeneration,
                                          SocketLifecycleState p_state,
                                          bool p_destroy,
                                          bool p_notify) noexcept {
    if (!IsCurrentSocketGeneration(p_expectedGeneration, p_currentGeneration) ||
        p_state == SocketLifecycleState::Destroyed ||
        p_state == SocketLifecycleState::Closing ||
        (p_state == SocketLifecycleState::Disconnected && !p_destroy)) {
        return {};
    }
    return {true, p_notify && p_state == SocketLifecycleState::Connected};
}
