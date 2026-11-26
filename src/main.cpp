#include <RED4ext/RED4ext.hpp>

#include "Version.h"
#include "Logger.h"
#include "SocketService.h"
#include "RunningStateCallback.h"
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
#include "RunningStateDiagnostics.h"
#endif

namespace {
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
    RunningStateDiagnostics g_runningStateDiagnostics;

    const char* DiagnosticEventName(RunningStateDiagnostics::Event p_event) {
        switch (p_event) {
            case RunningStateDiagnostics::Event::Enter: return "enter";
            case RunningStateDiagnostics::Event::UpdateSample: return "update_sample";
            case RunningStateDiagnostics::Event::Exit: return "exit";
            case RunningStateDiagnostics::Event::Unload: return "unload";
        }
        return "unknown";
    }

    void EmitRunningStateDiagnostic(const RunningStateDiagnostics::Record& p_record) noexcept {
        try {
            Logger::Info(
                "[RedSocketDiag] event={} enter={} update={} exit={} unload={} elapsed_us={} thread_slot={} observed_threads={} thread_slots_exhausted={} suppressed_logs={}",
                DiagnosticEventName(p_record.event), p_record.enterCount, p_record.updateCount,
                p_record.exitCount, p_record.unloadCount, p_record.elapsedMicroseconds,
                static_cast<int>(p_record.threadSlot), p_record.observedThreadCount,
                p_record.threadSlotsExhausted, p_record.suppressedLogCount);
        } catch (...) {
            // A diagnostic formatting/logging failure must not escape to RED4ext.
        }
    }
#endif

    bool OnStart(RED4ext::CGameApplication*) {
        auto& service = SocketService::Get();
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
        const bool terminal = service.IsUnloaded() || g_runningStateDiagnostics.IsClosed();
        return RunPersistentStateCallback(terminal, [&] {
            g_runningStateDiagnostics.OnEnter(EmitRunningStateDiagnostic);
            service.StartSession();
        });
#else
        return RunPersistentStateCallback(service.IsUnloaded(), [&] { service.StartSession(); });
#endif
    }

    bool OnUpdate(RED4ext::CGameApplication*) {
        auto& service = SocketService::Get();
        auto lease = service.BeginUpdate();
        if (!lease) return service.IsUnloaded();
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
        const bool terminal = service.IsUnloaded() || g_runningStateDiagnostics.IsClosed();
        return RunPersistentStateCallback(terminal, [&] {
            g_runningStateDiagnostics.OnUpdate(EmitRunningStateDiagnostic);
            service.Update(lease);
        });
#else
        return RunPersistentStateCallback(service.IsUnloaded(), [&] { service.Update(lease); });
#endif
    }

    bool OnStop(RED4ext::CGameApplication*) {
        auto& service = SocketService::Get();
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
        const bool terminal = service.IsUnloaded() || g_runningStateDiagnostics.IsClosed();
        return RunPersistentStateCallback(terminal, [&] {
            g_runningStateDiagnostics.OnExit(EmitRunningStateDiagnostic);
            service.Stop();
        });
#else
        return RunPersistentStateCallback(service.IsUnloaded(), [&] { service.Stop(); });
#endif
    }
}

RED4EXT_C_EXPORT bool RED4EXT_CALL Main(RED4ext::v1::PluginHandle p_handle, RED4ext::v1::EMainReason p_reason, const RED4ext::v1::Sdk* p_sdk) {
    try {
    switch (p_reason)  {
        case RED4ext::v1::EMainReason::Load: {
            Logger::Load(p_handle, p_sdk);
            RED4ext::v1::GameState state{};

            state.OnEnter = &OnStart;
            state.OnUpdate = &OnUpdate;
            state.OnExit = &OnStop;
            if (!p_sdk->gameStates->Add(p_handle, Red::EGameStateType::Running, &state))
                return false;
            Red::TypeInfoRegistrar::RegisterDiscovered();
            try { Logger::Info("DeepBide-RedSocket {}: Running callbacks registered.", REDSOCKET_VERSION); } catch (...) {}
            break;
        }
        case RED4ext::v1::EMainReason::Unload: {
#if defined(REDSOCKET_RUNNING_DIAGNOSTICS)
            g_runningStateDiagnostics.OnUnload(EmitRunningStateDiagnostic);
#endif
            const bool complete = SocketService::Get().Unload();
            if (complete) {
                try { Logger::Info("DeepBide-RedSocket {}: terminal cleanup complete.", REDSOCKET_VERSION); } catch (...) {}
            }
            return complete;
        }
    }
    return true;
    } catch (...) { return false; }
}

RED4EXT_C_EXPORT void RED4EXT_CALL Query(RED4ext::v1::PluginInfo* p_info) {
    p_info->name = L"RedSocket";
    p_info->author = L"DeepBide (RedSocket MIT fork; original by Louis Poirier)";
    p_info->version = RED4EXT_V1_SEMVER(REDSOCKET_MAJOR, REDSOCKET_MINOR, REDSOCKET_PATCH);
    p_info->runtime = RED4EXT_V1_RUNTIME_VERSION_LATEST;
    p_info->sdk = RED4EXT_V1_SDK_VERSION_CURRENT;
}

RED4EXT_C_EXPORT uint32_t RED4EXT_CALL Supports()
{
    return RED4EXT_API_VERSION_1;
}
