#pragma once

#include "SocketGeneration.h"

#include <cstdint>

enum class SocketCallbackKind : uint8_t { Connection, Disconnection };

// Generation gate at synchronous callback delivery on the update pump.
// RedSocket additionally allows already-admitted complete lines at clean EOF.
constexpr bool ShouldDeliverSocketCallback(SocketCallbackKind p_kind,
                                           uint64_t p_eventGeneration,
                                           uint64_t p_currentGeneration,
                                           SocketLifecycleState p_currentState,
                                           bool p_destroyed,
                                           bool p_sessionCurrent) noexcept {
    if (p_destroyed || !p_sessionCurrent || p_eventGeneration != p_currentGeneration)
        return false;
    if (p_kind == SocketCallbackKind::Connection)
        return p_currentState == SocketLifecycleState::Connected ||
               p_currentState == SocketLifecycleState::Disconnected;
    return p_currentState == SocketLifecycleState::Disconnected;
}
