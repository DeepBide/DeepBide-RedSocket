#pragma once

#include <atomic>
#include <array>
#include <cstdint>
#include <deque>
#include <mutex>
#include <memory>
#include <string>

#include <asio.hpp>
#include <RED4ext/RED4ext.hpp>
#include <RED4ext/Scripting/IScriptable.hpp>
#include <RedLib.hpp>

#include "SocketOwner.h"
#include "SocketGeneration.h"
#include "SocketCallbackGate.h"
#include "SocketTransportBuffers.h"

inline constexpr std::size_t MAX_PENDING_WRITE_BYTES = 1024 * 1024;
inline constexpr std::size_t MAX_RECEIVE_BUFFER_BYTES = 1024 * 1024;

class RedSocket : public Red::IScriptable {
#if defined(DEEPBIDE_NATIVE_TEST)
    friend struct SocketTestAccess;
    std::atomic_bool m_failResolveForTest{false}, m_failWriteForTest{false};
#endif
    struct RedCallback {
        Red::Handle<Red::IScriptable> Object;
        Red::CName OnCommand;
        Red::CName OnConnection;
        Red::CName OnDisconnection;
    };

    struct PendingWrite {
        std::shared_ptr<const SocketWritePayload> Payload;
        uint64_t SendErrorId{0};
    };

    const bool m_isTracked;
    const uint64_t m_eventIdentity;

    // The worker thread is the sole caller of every resolver/socket operation.
    // Public methods enqueue commands. Exceptional post-join retirement
    // can close and drain canceled I/O exclusively on an external caller.
    SocketOwner m_owner;
    std::shared_ptr<SocketTransportEpoch> m_transport;
    std::mutex m_shutdownMutex;

    // Protects only script callback registrations. It is never held while
    // polling Asio or invoking a RED virtual callback.
    mutable std::mutex m_callbackMutex;
    RedCallback m_callback;
    bool m_connectPending{false};
    std::atomic_bool m_disconnectPending{false};
    std::atomic<SocketLifecycleState> m_lifecycle{SocketLifecycleState::Disconnected};
    std::atomic<uint64_t> m_generation{0};
    std::atomic<uint64_t> m_cleanEofGeneration{0};
    std::atomic<uint64_t> m_connectionSessionEpoch{0};
    std::atomic<uint32_t> m_pendingTerminalEvents{0};
    std::atomic_bool m_isConnected{false};
    std::atomic_bool m_shutdownStarted{false};

    std::deque<PendingWrite> m_writeQueue;
    std::size_t m_pendingWriteBytes{0};
    bool m_writeInProgress{false};

    void StartConnect(std::string p_ipAddress, uint16_t p_port, uint64_t p_sessionEpoch);
    void FailConnect(uint64_t generation, int32_t status);
    void StartRead(uint64_t p_generation);
    void HandleRead(uint64_t p_generation, const std::shared_ptr<SocketTransportEpoch>& transport,
                    const asio::error_code& p_error, std::size_t p_length);
    void StartWrite(uint64_t p_generation);
    void HandleWrite(uint64_t p_generation, const asio::error_code& p_error, std::size_t p_length);
    void CloseOnExecutor(uint64_t p_expectedGeneration, bool p_destroy, bool p_notify, bool cleanEof = false);
    bool CanDrainCleanEof(uint64_t generation) const noexcept;
    void DispatchConnection(uint64_t p_generation, int32_t p_status, bool alreadyReserved = false);
    void DispatchDisconnection(uint64_t p_generation, bool alreadyReserved = false);
    void DispatchCommand(uint64_t p_generation, std::string p_command);
    void QueueRuntimeLog(std::string_view p_message, int p_level) noexcept;
    void ClearRegistration();
    void Shutdown();

public:
    explicit RedSocket(bool p_isTracked = false);
    ~RedSocket() override;

    static Red::Handle<RedSocket> Create();
    static void Destroy(const Red::Handle<RedSocket>& p_socket);

    bool IsTracked() const;
    uint64_t GetEventIdentity() const;
    bool IsConnected() const;
    bool IsRetired() const;
    bool HasListener() const;

    void RegisterListener(const Red::Handle<Red::IScriptable>& p_object, const Red::CName& p_onCommand,
                          const Red::CName& p_onConnection,
                          const Red::Optional<Red::CName>& p_onDisconnection);
    bool TryRegisterListener(const Red::Handle<Red::IScriptable>& p_object, const Red::CName& p_onCommand,
                             const Red::CName& p_onConnection, const Red::Optional<Red::CName>& p_onDisconnection);
    void UnregisterListener();

    void Connect(const Red::CString& p_ipAddress, uint16_t p_port);
    void Disconnect();

    // Retained as no-op compatibility entry points for consumers that invoked
    // the old service-driven polling API. Socket I/O runs on SocketOwner.
    // These C++-only shims are not exposed to scripts.
    void Run();
    void Poll();
    void SendCommand(const Red::CString& p_command,
                     const Red::Optional<int32_t, 10>& p_limit,
                     const Red::Optional<Red::Handle<Red::IScriptable>>& p_target,
                     const Red::Optional<Red::CName>& p_error);

    RTTI_IMPL_TYPEINFO(RedSocket)
    RTTI_IMPL_ALLOCATOR()
};

RTTI_DEFINE_CLASS(RedSocket, "Socket", {
    RTTI_ALIAS("RedSocket.Socket");

    RTTI_METHOD(Create);
    RTTI_METHOD(Destroy);

    RTTI_METHOD(IsConnected);
    RTTI_METHOD(IsRetired);

    RTTI_METHOD(RegisterListener);
    RTTI_METHOD(TryRegisterListener);
    RTTI_METHOD(UnregisterListener);

    RTTI_METHOD(Connect);
    RTTI_METHOD(Disconnect);

    RTTI_METHOD(SendCommand);
})
