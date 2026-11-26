#pragma once

#include <utility>

// RED4ext StateSystem::Run removes a callback when it returns true. Keep the
// Running callbacks registered across ticks and state re-entry; after terminal
// unload the supplied action is skipped and the callback may be removed.
template <class Action>
bool RunPersistentStateCallback(bool p_terminal, Action&& p_action) noexcept {
    if (p_terminal) return true;
    // Keep exceptions from crossing the host callback ABI. A failed action
    // is not a completed lifecycle transition; retain the callback.
    try { std::forward<Action>(p_action)(); } catch (...) {}
    return false;
}
