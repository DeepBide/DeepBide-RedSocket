#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <utility>

#ifndef REDSOCKET_DIAGNOSTICS_UPDATE_SAMPLE_INTERVAL
#define REDSOCKET_DIAGNOSTICS_UPDATE_SAMPLE_INTERVAL 128
#endif

class RunningStateDiagnostics {
public:
    enum class Event : uint8_t { Enter, UpdateSample, Exit, Unload };

    struct Record {
        Event event{};
        uint64_t enterCount{0};
        uint64_t updateCount{0};
        uint64_t exitCount{0};
        uint64_t unloadCount{0};
        uint64_t elapsedMicroseconds{0};
        uint64_t suppressedLogCount{0};
        int8_t threadSlot{-1};
        uint8_t observedThreadCount{0};
        bool threadSlotsExhausted{false};
    };

    struct Counters {
        uint64_t enter{0};
        uint64_t update{0};
        uint64_t exit{0};
        uint64_t unload{0};
        uint64_t suppressedLogs{0};
        bool closed{false};
    };

    static constexpr uint8_t MaxThreadSlots = 8;
    static constexpr uint8_t MaxLogLines = 32;

#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
    static constexpr bool Enabled = true;
    static constexpr uint64_t CompiledSampleInterval =
        REDSOCKET_DIAGNOSTICS_UPDATE_SAMPLE_INTERVAL;

    explicit RunningStateDiagnostics(uint64_t p_sampleInterval = CompiledSampleInterval,
                                     uint8_t p_maxLogLines = MaxLogLines)
        : m_started(std::chrono::steady_clock::now()),
          m_sampleInterval(std::max<uint64_t>(1, p_sampleInterval)),
          m_maxLogLines(std::clamp<uint8_t>(p_maxLogLines, 1, MaxLogLines)) {}

    template<class Emit>
    void OnEnter(Emit&& p_emit) { RecordEdge(Event::Enter, std::forward<Emit>(p_emit)); }

    template<class Emit>
    void OnUpdate(Emit&& p_emit) {
        uint64_t updateCount = 0;
        auto state = m_updateState.load(std::memory_order_relaxed);
        for (;;) {
            if ((state & kClosedFlag) != 0) return;
            const auto count = state & kCountMask;
            if (count == kCountMask) return;
            if (m_updateState.compare_exchange_weak(state, state + 1,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                updateCount = count + 1;
                break;
            }
        }
        if (updateCount % m_sampleInterval != 0) return;
        std::lock_guard lock(m_mutex);
        if (m_closed) return;
        EmitIfRoom(Event::UpdateSample, std::forward<Emit>(p_emit));
    }

    template<class Emit>
    void OnExit(Emit&& p_emit) { RecordEdge(Event::Exit, std::forward<Emit>(p_emit)); }

    template<class Emit>
    void OnUnload(Emit&& p_emit) {
        std::lock_guard lock(m_mutex);
        if (m_closed) return;
        ++m_unloadCount;
        m_closed = true;
        m_updateState.fetch_or(kClosedFlag, std::memory_order_relaxed);
        // Reserve the final line for unload, even when all normal lines were used.
        EmitRecord(Event::Unload, std::forward<Emit>(p_emit));
    }

    bool IsClosed() const {
        std::lock_guard lock(m_mutex);
        return m_closed;
    }

    Counters GetCounters() const {
        std::lock_guard lock(m_mutex);
        return {m_enterCount, CurrentUpdateCount(), m_exitCount, m_unloadCount,
                m_suppressedLogCount, m_closed};
    }

private:
    template<class Emit>
    void RecordEdge(Event p_event, Emit&& p_emit) {
        std::lock_guard lock(m_mutex);
        if (m_closed) return;
        switch (p_event) {
            case Event::Enter: ++m_enterCount; break;
            case Event::Exit: ++m_exitCount; break;
            default: return;
        }
        EmitIfRoom(p_event, std::forward<Emit>(p_emit));
    }

    template<class Emit>
    void EmitIfRoom(Event p_event, Emit&& p_emit) {
        if (m_emittedLogCount >= static_cast<uint8_t>(m_maxLogLines - 1)) {
            ++m_suppressedLogCount;
            return;
        }
        ++m_emittedLogCount;
        EmitRecord(p_event, std::forward<Emit>(p_emit));
    }

    template<class Emit>
    void EmitRecord(Event p_event, Emit&& p_emit) {
        Record record;
        record.event = p_event;
        record.enterCount = m_enterCount;
        record.updateCount = CurrentUpdateCount();
        record.exitCount = m_exitCount;
        record.unloadCount = m_unloadCount;
        record.elapsedMicroseconds = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - m_started).count());
        record.suppressedLogCount = m_suppressedLogCount;
        record.threadSlot = GetThreadSlot(std::this_thread::get_id());
        record.observedThreadCount = m_threadCount;
        record.threadSlotsExhausted = m_threadSlotsExhausted;
        try {
            std::forward<Emit>(p_emit)(record);
        } catch (...) {
            // Diagnostics must never throw through a RED4ext state callback.
        }
    }

    int8_t GetThreadSlot(std::thread::id p_thread) {
        for (uint8_t i = 0; i < m_threadCount; ++i) {
            if (m_threadSlots[i] == p_thread) return static_cast<int8_t>(i);
        }
        if (m_threadCount == MaxThreadSlots) {
            m_threadSlotsExhausted = true;
            return -1;
        }
        const auto slot = m_threadCount++;
        m_threadSlots[slot] = p_thread;
        return static_cast<int8_t>(slot);
    }

    uint64_t CurrentUpdateCount() const noexcept {
        return m_updateState.load(std::memory_order_relaxed) & kCountMask;
    }

    static constexpr uint64_t kClosedFlag = uint64_t{1} << 63;
    static constexpr uint64_t kCountMask = ~kClosedFlag;
    mutable std::mutex m_mutex;
    std::atomic<uint64_t> m_updateState{0};
    const std::chrono::steady_clock::time_point m_started;
    const uint64_t m_sampleInterval;
    const uint8_t m_maxLogLines;
    uint64_t m_enterCount{0};
    uint64_t m_exitCount{0};
    uint64_t m_unloadCount{0};
    uint64_t m_suppressedLogCount{0};
    uint8_t m_emittedLogCount{0};
    uint8_t m_threadCount{0};
    std::array<std::thread::id, MaxThreadSlots> m_threadSlots{};
    bool m_threadSlotsExhausted{false};
    bool m_closed{false};
#else
    static constexpr bool Enabled = false;
    static constexpr uint64_t CompiledSampleInterval =
        REDSOCKET_DIAGNOSTICS_UPDATE_SAMPLE_INTERVAL;

    explicit RunningStateDiagnostics(uint64_t = CompiledSampleInterval, uint8_t = 32) {}

    template<class Emit> void OnEnter(Emit&&) noexcept {}
    template<class Emit> void OnUpdate(Emit&&) noexcept {}
    template<class Emit> void OnExit(Emit&&) noexcept {}
    template<class Emit> void OnUnload(Emit&&) noexcept {}
    bool IsClosed() const noexcept { return false; }
    Counters GetCounters() const noexcept { return {}; }
#endif
};
