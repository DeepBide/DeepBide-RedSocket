#pragma once

#include <atomic>
#include <cstddef>
#include <memory>

// Shared across all socket owners and pending writes. Accounted bytes are a
// conservative admission bound, not a claim about total process RSS.
class SocketResourceBudget {
public:
    static constexpr std::size_t kLimit = 64 * 1024 * 1024;
    static constexpr std::size_t kEntryOverhead = 128;

    class Reservation {
    public:
        explicit Reservation(std::size_t bytes) noexcept : m_bytes(bytes) {}
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        ~Reservation() { s_used.fetch_sub(m_bytes, std::memory_order_acq_rel); }
    private:
        std::size_t m_bytes;
    };

    static std::shared_ptr<const Reservation> Reserve(std::size_t payload) noexcept {
        if (payload > kLimit - kEntryOverhead) return {};
        const auto bytes = payload + kEntryOverhead;
        auto used = s_used.load(std::memory_order_acquire);
        do {
            if (bytes > kLimit - used) return {};
        } while (!s_used.compare_exchange_weak(used, used + bytes,
            std::memory_order_acq_rel, std::memory_order_acquire));
        try { return std::make_shared<const Reservation>(bytes); }
        catch (...) { s_used.fetch_sub(bytes, std::memory_order_acq_rel); return {}; }
    }

    static std::size_t Used() noexcept { return s_used.load(std::memory_order_acquire); }
private:
    inline static std::atomic<std::size_t> s_used{0};
};
