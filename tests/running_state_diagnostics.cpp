#include "../src/RunningStateDiagnostics.h"

#include <array>
#include <barrier>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
void Check(bool p_condition, const char* p_expression, int p_line) {
    if (p_condition) return;
    std::cerr << "CHECK failed at line " << p_line << ": " << p_expression << '\n';
    std::exit(2);
}
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--probe-check-failure") CHECK(false);
    CHECK(RunningStateDiagnostics::CompiledSampleInterval == 128);

    RunningStateDiagnostics diagnostics(3, 5);
    std::vector<RunningStateDiagnostics::Record> records;
    const auto emit = [&](const RunningStateDiagnostics::Record& p_record) {
        records.push_back(p_record);
    };

    diagnostics.OnEnter(emit);
    for (unsigned tick = 0; tick != 10; ++tick) diagnostics.OnUpdate(emit);
    diagnostics.OnExit(emit);

    if constexpr (RunningStateDiagnostics::Enabled) {
        const auto beforeUnload = diagnostics.GetCounters();
        CHECK(beforeUnload.enter == 1 && beforeUnload.update == 10 && beforeUnload.exit == 1);
        CHECK(beforeUnload.unload == 0 && beforeUnload.suppressedLogs == 1);
        CHECK(records.size() == 4); // enter + three samples; cap reserves one line for unload
        CHECK(records[0].event == RunningStateDiagnostics::Event::Enter);
        CHECK(records[1].event == RunningStateDiagnostics::Event::UpdateSample);
        CHECK(records[1].updateCount == 3);
        CHECK(records[2].event == RunningStateDiagnostics::Event::UpdateSample);
        CHECK(records[2].updateCount == 6);
        CHECK(records[3].event == RunningStateDiagnostics::Event::UpdateSample);
        CHECK(records[3].updateCount == 9);
        for (const auto& record : records) {
            CHECK(record.elapsedMicroseconds <= records.back().elapsedMicroseconds);
            CHECK(record.threadSlot >= 0);
            CHECK(record.observedThreadCount <= RunningStateDiagnostics::MaxThreadSlots);
        }

        diagnostics.OnUnload(emit);
        CHECK(diagnostics.IsClosed());
        CHECK(records.size() == 5);
        CHECK(records.back().event == RunningStateDiagnostics::Event::Unload);
        CHECK(records.back().unloadCount == 1);
        CHECK(records.back().suppressedLogCount == 1);
        const auto afterUnload = diagnostics.GetCounters();
        CHECK(afterUnload.enter == 1 && afterUnload.update == 10 && afterUnload.exit == 1);
        CHECK(afterUnload.unload == 1 && afterUnload.closed);

        diagnostics.OnEnter(emit);
        diagnostics.OnUpdate(emit);
        diagnostics.OnExit(emit);
        diagnostics.OnUnload(emit);
        const auto afterLateCallbacks = diagnostics.GetCounters();
        CHECK(afterLateCallbacks.enter == afterUnload.enter);
        CHECK(afterLateCallbacks.update == afterUnload.update);
        CHECK(afterLateCallbacks.exit == afterUnload.exit);
        CHECK(afterLateCallbacks.unload == afterUnload.unload);
        CHECK(records.size() == 5);

        RunningStateDiagnostics threadDiagnostics(1, 32);
        constexpr std::size_t threadCount = RunningStateDiagnostics::MaxThreadSlots + 4;
        std::barrier<> start(static_cast<std::ptrdiff_t>(threadCount));
        std::mutex recordsMutex;
        std::vector<RunningStateDiagnostics::Record> threadRecords;
        std::array<std::thread, threadCount> workers;
        for (auto& worker : workers) {
            worker = std::thread([&] {
                start.arrive_and_wait();
                threadDiagnostics.OnEnter([&](const auto& record) {
                    std::lock_guard lock(recordsMutex);
                    threadRecords.push_back(record);
                });
            });
        }
        for (auto& worker : workers) worker.join();
        CHECK(threadDiagnostics.GetCounters().enter == threadCount);
        CHECK(threadRecords.size() == threadCount);
        bool sawExhaustion = false;
        for (const auto& record : threadRecords) {
            CHECK(record.observedThreadCount <= RunningStateDiagnostics::MaxThreadSlots);
            sawExhaustion = sawExhaustion || record.threadSlotsExhausted;
            CHECK(record.threadSlot < static_cast<int8_t>(RunningStateDiagnostics::MaxThreadSlots));
        }
        CHECK(sawExhaustion);

        RunningStateDiagnostics concurrentUpdates(100000, 2);
        constexpr unsigned updateWorkers = 8;
        constexpr unsigned updatesPerWorker = 5000;
        std::array<std::thread, updateWorkers> updateThreads;
        for (auto& worker : updateThreads) {
            worker = std::thread([&] {
                for (unsigned i = 0; i != updatesPerWorker; ++i) {
                    concurrentUpdates.OnUpdate([](const auto&) {});
                }
            });
        }
        for (auto& worker : updateThreads) worker.join();
        concurrentUpdates.OnUnload([](const auto&) {});
        CHECK(concurrentUpdates.GetCounters().update == updateWorkers * updatesPerWorker);
        std::cout << "RUNNING_DIAGNOSTICS_PASS enabled=1 updates=10 sample=3 max_logs=5 thread_slots=8 late_callbacks=ignored\n";
    } else {
        CHECK(!diagnostics.IsClosed());
        CHECK(records.empty());
        const auto counters = diagnostics.GetCounters();
        CHECK(counters.enter == 0 && counters.update == 0 && counters.exit == 0 && counters.unload == 0);
        diagnostics.OnUnload(emit);
        diagnostics.OnUpdate(emit);
        CHECK(records.empty() && !diagnostics.IsClosed());
        std::cout << "RUNNING_DIAGNOSTICS_PASS enabled=0 no_state_no_logs\n";
    }
    return 0;
}
