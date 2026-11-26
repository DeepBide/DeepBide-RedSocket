#pragma once

#include <cstdint>

// Models the plugin service's lifetime independently of RED4ext callbacks.
// Leaving Running ends a session; only DLL unload is terminal.
class SocketSessionGate {
public:
    bool StartSession() noexcept {
        if (m_unloaded) return false;
        if (!m_active) ++m_epoch;
        m_active = true;
        return true;
    }

    void StopSession() noexcept {
        if (m_active) ++m_epoch;
        m_active = false;
    }

    void Unload() noexcept {
        if (m_active) ++m_epoch;
        m_active = false;
        m_unloaded = true;
    }

    bool Accepting() const noexcept { return m_active && !m_unloaded; }
    bool IsUnloaded() const noexcept { return m_unloaded; }
    uint64_t Epoch() const noexcept { return m_epoch; }
    bool IsCurrent(uint64_t p_epoch) const noexcept {
        return Accepting() && p_epoch == m_epoch;
    }

private:
    bool m_active{false};
    bool m_unloaded{false};
    uint64_t m_epoch{0};
};
