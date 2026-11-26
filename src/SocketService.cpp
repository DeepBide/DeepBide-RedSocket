#include "SocketService.h"

#include "Logger.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace {
constexpr std::size_t EVENT_OVERHEAD_BUDGET = 512;
constexpr std::size_t MAX_SEND_ERROR_CALLBACKS = 4096;
}

bool SocketService::IsTrackedLocked(RedSocket* socket, uint64_t identity) {
    for (const auto& current : m_sockets)
        if (current.GetPtr() == socket && current->GetEventIdentity() == identity) return true;
    return false;
}

bool SocketService::Track(const Red::Handle<RedSocket>& p_socket) {
    if (!p_socket || !p_socket->IsTracked()) return false;
    std::lock_guard lock(m_mutex);
    if (!m_session.Accepting() || m_sockets.size() >= SocketEventBudget::kMaxSockets)
        return false;
    m_sockets.push_back(p_socket);
    return true;
}

void SocketService::Untrack(const Red::Handle<RedSocket>& p_socket) {
    if (!p_socket || !p_socket->IsTracked()) return;
    std::lock_guard lock(m_mutex);
    const auto identity = p_socket->GetEventIdentity();
    m_eventBudget.PurgeIdentity(m_pendingEvents, identity);
    m_sendErrors.RequestClearIf([identity](const auto& meta) { return meta.SocketIdentity == identity; });
    std::erase(m_sockets, p_socket);
}

void SocketService::Retire(RedSocket* socket, uint64_t identity) {
    // Called only by an external native caller, not an I/O worker.
    Red::Handle<RedSocket> owned;
    {
        std::lock_guard lock(m_mutex);
        for (const auto& current : m_sockets) {
            if (current.GetPtr() == socket && current->GetEventIdentity() == identity) {
                owned = current;
                break;
            }
        }
    }
    if (owned) RedSocket::Destroy(owned);
}

void SocketService::Update(SocketUpdateQuiescence::Lease& lease) {
    if (!lease) return;
    const auto dispatchStarted = std::chrono::steady_clock::now();
    {
        // RED references and event captures are released before cleanup and
        // while the outer host callback still holds its exclusive lease.
        auto releasedSendErrors = m_sendErrors.DrainCleared();
        std::size_t dispatchedEvents = 0;
        std::size_t dispatchedBytes = 0;
        for (;;) {
            PendingEvent event;
            Red::Handle<RedSocket> socket;
            bool invalidCapacity = false;
            {
                std::lock_guard lock(m_mutex);
                if (!m_session.Accepting() || m_pendingEvents.empty()) break;
                const auto elapsed = std::chrono::steady_clock::now() - dispatchStarted;
                SocketEventDispatchScheduler::SelectionDiagnostics diagnostics;
                const auto next = SocketEventDispatchScheduler::SelectNext(
                    m_pendingEvents, dispatchedEvents, dispatchedBytes, elapsed,
                    m_criticalDispatchStreak, &diagnostics);
                invalidCapacity = diagnostics.CapacityExceeded;
                if (!invalidCapacity) {
                    if (next == m_pendingEvents.end()) break;
                    event = std::move(*next);
                    m_pendingEvents.erase(next);
                    m_eventBudget.Release(event.Identity, event.AccountedBytes, event.Critical);
                    m_criticalDispatchStreak = event.Critical
                        ? std::min(m_criticalDispatchStreak + 1,
                            SocketEventDispatchScheduler::kMaxConsecutiveCritical) : 0;
                    // Resolve at dispatch, not against a snapshot taken
                    // before another socket may have been created.
                    for (const auto& current : m_sockets) {
                        if (current.GetPtr() == event.Socket && current->GetEventIdentity() == event.Identity) {
                            socket = current;
                            break;
                        }
                    }
                }
            }
            if (invalidCapacity) {
                // Internal invariant corruption, not ordinary queue saturation.
                Stop();
                break;
            }
            if (!IsSessionAccepting()) break;
            if (socket && !socket->IsRetired()) {
                try { event.Deliver(socket); }
                catch (...) {
                    // An exception in one script callback must not abandon
                    // unrelated admitted work or escape the host pump.
                    try { Logger::Error("RedSocket callback failed."); } catch (...) {}
                }
            }
            ++dispatchedEvents;
            dispatchedBytes += event.AccountedBytes;
        }
    }
    // No release-and-reacquire gap: external unload cannot observe zero
    // active leases before these resources and deferred stop are retired.
    if (m_stopCleanupPending.exchange(false, std::memory_order_acq_rel))
        CleanupStoppedResources();
}

bool SocketService::QueueEvent(
    RedSocket* p_socket, uint64_t p_identity,
    std::function<void(const Red::Handle<RedSocket>&)> p_deliver,
    std::size_t p_payloadBytes, bool p_critical) {
    if (p_payloadBytes > SocketEventDispatchBudget::kMaxBytesPerUpdate - EVENT_OVERHEAD_BUDGET)
        return false;
    const auto accountedBytes = EVENT_OVERHEAD_BUDGET + p_payloadBytes;
    std::lock_guard lock(m_mutex);
    if (!m_session.Accepting() || !IsTrackedLocked(p_socket, p_identity)) return false;
    // Critical transitions have a per-socket reserve. Refusal affects only
    // this identity; optional send errors use the normal lane.
    return m_eventBudget.TryEnqueue(p_identity, accountedBytes, p_critical, [&] {
        m_pendingEvents.push_back(PendingEvent{
            p_socket, p_identity, accountedBytes, p_critical, std::move(p_deliver)});
    });
}

uint64_t SocketService::RegisterSendError(const Red::Handle<Red::IScriptable>& p_target,
                                         const Red::CName& p_callback,
                                         RedSocket* socket, uint64_t identity) {
    if (!p_target || p_callback.IsNone()) return 0;
    std::lock_guard lock(m_mutex);
    if (!m_session.Accepting() || !IsTrackedLocked(socket, identity) ||
        m_sendErrors.Size() >= MAX_SEND_ERROR_CALLBACKS || m_nextSendErrorId == 0)
        return 0;
    const auto id = m_nextSendErrorId++;
    Red::Handle<Red::IScriptable> retained = p_target;
    const bool inserted = m_sendErrors.Insert(id, std::move(retained),
        SendErrorMetadata{p_callback, m_session.Epoch(), identity}, MAX_SEND_ERROR_CALLBACKS);
    return inserted ? id : 0;
}

void SocketService::QueueSendError(uint64_t p_id, RedSocket* socket, uint64_t p_identity) {
    if (p_id == 0) return;
    bool queued = false;
    try {
        queued = QueueEvent(socket, p_identity, [this, p_id, p_identity](const Red::Handle<RedSocket>&) {
            DeliverSendError(p_id, p_identity);
        });
    } catch (...) {}
    if (!queued) ClearSendError(p_id);
}

void SocketService::ClearSendError(uint64_t p_id) {
    m_sendErrors.RequestClear(p_id);
}

void SocketService::DeliverSendError(uint64_t p_id, uint64_t identity) {
    SendErrorRegistry::Entry entry;
    if (!m_sendErrors.Take(p_id, entry)) return;
    bool current = false;
    {
        std::lock_guard lock(m_mutex);
        if (entry.Metadata.SocketIdentity == identity && m_session.IsCurrent(entry.Metadata.SessionEpoch)) {
            for (const auto& socket : m_sockets) {
                if (socket->GetEventIdentity() == identity && !socket->IsRetired()) {
                    current = true;
                    break;
                }
            }
        }
    }
    if (current && entry.Handle && !entry.Metadata.Callback.IsNone())
        Red::CallVirtual(entry.Handle.GetPtr(), entry.Metadata.Callback);
}

void SocketService::StartSession() {
    bool started = false;
    const bool transitioned = m_updateQuiescence.CloseAndRunIfQuiescent([this, &started] {
        if (m_stopCleanupPending.exchange(false)) CleanupStoppedResources();
        std::lock_guard lock(m_mutex);
        started = m_session.StartSession();
        if (started) m_criticalDispatchStreak = 0;
        return started;
    });
    if (!transitioned) Logger::Error("RedSocket Running re-entry deferred: update or cleanup is active.");
    else if (!started) Logger::Error("RedSocket Running re-entry refused after terminal unload.");
    else { try { Logger::Info("RedSocket Running session started."); } catch (...) {} }
}

void SocketService::Stop() {
    if (!m_updateQuiescence.CloseAndRunIfQuiescent([this] {
            {
                std::lock_guard lock(m_mutex);
                m_session.StopSession();
            }
            m_stopCleanupPending.store(false);
            CleanupStoppedResources();
        })) {
        std::lock_guard lock(m_mutex);
        m_session.StopSession();
        m_stopCleanupPending.store(true);
    }
}

void SocketService::CleanupStoppedResources() {
    std::lock_guard lifecycleLock(m_lifecycleMutex);
    std::vector<Red::Handle<RedSocket>> sockets;
    {
        std::lock_guard lock(m_mutex);
        sockets.swap(m_sockets);
        m_pendingEvents.clear();
        m_eventBudget.Reset();
        m_criticalDispatchStreak = 0;
    }
    // Bulk release is on the external lifecycle/update path, not the worker.
    m_sendErrors.ReleaseAll();
    for (auto& socket: sockets) RedSocket::Destroy(socket);
    const auto count = sockets.size();
    sockets.clear();
    try { Logger::Info("RedSocket session cleanup complete; retired sockets={}", count); } catch (...) {}
}

bool SocketService::Unload() {
    if (!m_updateQuiescence.CloseAndRunIfQuiescent([this] {
            {
                std::lock_guard lock(m_mutex);
                m_session.Unload();
            }
            m_stopCleanupPending.store(false);
            CleanupStoppedResources();
        })) {
        // RED4ext v1.31.1 PluginSystem::Unload ignores Main's return value.
        // Refuse success and defer cleanup, but this is NOT an unload veto.
        // Only the loader's ordinary process shutdown route is a candidate
        // for qualification; dynamic/reentrant unmapping is unsupported.
        {
            std::lock_guard lock(m_mutex);
            m_session.Unload();
            m_stopCleanupPending.store(true);
        }
        try { Logger::Error("RedSocket reentrant unload: unsupported loader path; return is not a veto."); } catch (...) {}
        return false;
    }
    return true;
}

bool SocketService::IsUnloaded() {
    std::lock_guard lock(m_mutex);
    return m_session.IsUnloaded();
}

bool SocketService::IsSessionAccepting() {
    std::lock_guard lock(m_mutex);
    return m_session.Accepting();
}

uint64_t SocketService::SessionEpoch() {
    std::lock_guard lock(m_mutex);
    return m_session.Epoch();
}

bool SocketService::IsSessionCurrent(uint64_t p_epoch) {
    std::lock_guard lock(m_mutex);
    return m_session.IsCurrent(p_epoch);
}
