#pragma once

// Asio must select Winsock2 before the SDK includes Windows headers.
#include <asio.hpp>
#include <RED4ext/RED4ext.hpp>
#include <RedLib.hpp>

#include <functional>
#include <cstddef>
#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "RedSocket.h"
#include "SocketEventBudget.h"
#include "SocketSessionGate.h"
#include "DeferredHandleRegistry.h"
#include "SocketUpdateQuiescence.h"

class SocketService {
    struct PendingEvent {
        RedSocket* Socket{nullptr};
        uint64_t Identity{0};
        std::size_t AccountedBytes{0};
        bool Critical{false};
        std::function<void(const Red::Handle<RedSocket>&)> Deliver;
    };

    std::vector<Red::Handle<RedSocket>> m_sockets;
    std::list<PendingEvent> m_pendingEvents;
    SocketEventBudget m_eventBudget;
    std::size_t m_criticalDispatchStreak{0};
    struct SendErrorMetadata {
        Red::CName Callback;
        uint64_t SessionEpoch{0};
        uint64_t SocketIdentity{0};
    };
    using SendErrorRegistry = DeferredHandleRegistry<Red::Handle<Red::IScriptable>, SendErrorMetadata>;
    SendErrorRegistry m_sendErrors;
    SocketUpdateQuiescence m_updateQuiescence;
    std::atomic<bool> m_stopCleanupPending{false};
    std::mutex m_lifecycleMutex;
    uint64_t m_nextSendErrorId{1};
    std::mutex m_mutex;
    bool IsTrackedLocked(RedSocket* socket, uint64_t identity);
    SocketSessionGate m_session;

public:
    static SocketService& Get() {
        static SocketService instance;

        return instance;
    }

    bool Track(const Red::Handle<RedSocket>& p_socket);
    void Untrack(const Red::Handle<RedSocket>& p_socket);

    void Retire(RedSocket* socket, uint64_t identity);
    SocketUpdateQuiescence::Lease BeginUpdate() { return m_updateQuiescence.TryEnterExclusive(); }
    void Update(SocketUpdateQuiescence::Lease& lease);
    void StartSession();
    void Stop();
    bool Unload();
    bool IsUnloaded();
    bool IsSessionAccepting();
    void CleanupStoppedResources();
    uint64_t SessionEpoch();
    bool IsSessionCurrent(uint64_t p_epoch);
    bool QueueEvent(RedSocket* p_socket, uint64_t p_identity,
                    std::function<void(const Red::Handle<RedSocket>&)> p_deliver,
                    std::size_t p_payloadBytes = 0, bool p_critical = false);
    uint64_t RegisterSendError(const Red::Handle<Red::IScriptable>& p_target, const Red::CName& p_callback,
                               RedSocket* socket, uint64_t identity);
    void QueueSendError(uint64_t p_id, RedSocket* socket, uint64_t p_identity);
    void ClearSendError(uint64_t p_id);
    void DeliverSendError(uint64_t p_id, uint64_t identity);
};
