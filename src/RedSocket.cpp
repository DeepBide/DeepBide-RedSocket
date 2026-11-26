#include "RedSocket.h"
#include "SocketService.h"

#include <algorithm>
#include <atomic>
#include <string>
#include <utility>

#include "Logger.h"

namespace {
std::atomic<uint64_t> g_nextEventIdentity{1};

class TerminalCallbackGuard {
public:
    explicit TerminalCallbackGuard(std::atomic<uint32_t>& p_count) : m_count(&p_count) {}
    TerminalCallbackGuard(const TerminalCallbackGuard&) = delete;
    TerminalCallbackGuard& operator=(const TerminalCallbackGuard&) = delete;
    ~TerminalCallbackGuard() { Release(); }
    void Release() noexcept {
        if (m_count) {
            m_count->fetch_sub(1, std::memory_order_acq_rel);
            m_count = nullptr;
        }
    }
private:
    std::atomic<uint32_t>* m_count;
};
}

RedSocket::RedSocket(bool p_isTracked) :
    m_isTracked(p_isTracked),
    m_eventIdentity(g_nextEventIdentity.fetch_add(1, std::memory_order_relaxed)) {
}

RedSocket::~RedSocket() {
    Shutdown();
}

Red::Handle<RedSocket> RedSocket::Create() {
    try {
        const auto socket = Red::MakeHandle<RedSocket>(true);
        if (!socket) return {};
        if (!SocketService::Get().Track(socket)) {
            socket->Shutdown();
            return {};
        }
        return socket;
    } catch (...) { return {}; }
}

void RedSocket::Destroy(const Red::Handle<RedSocket>& p_socket) {
    if (!p_socket) return;
    p_socket->Shutdown();
    SocketService::Get().Untrack(p_socket);
    p_socket->ClearRegistration();
}

bool RedSocket::IsTracked() const {
    return m_isTracked;
}

uint64_t RedSocket::GetEventIdentity() const {
    return m_eventIdentity;
}

bool RedSocket::IsConnected() const {
    return m_isConnected.load(std::memory_order_acquire);
}

bool RedSocket::IsRetired() const {
    return m_shutdownStarted.load(std::memory_order_acquire);
}

bool RedSocket::HasListener() const {
    std::lock_guard lock(m_callbackMutex);
    return m_callback.Object && !m_callback.OnCommand.IsNone() && !m_callback.OnConnection.IsNone();
}

void RedSocket::QueueRuntimeLog(std::string_view p_message, int p_level) noexcept {
    try {
        const auto identity = m_eventIdentity;
        SocketService::Get().QueueEvent(this, identity,
            [message = std::string(p_message), p_level, identity](const Red::Handle<RedSocket>&) {
                if (p_level == 0) Logger::Info("Socket {}: {}", identity, message);
                else if (p_level == 1) Logger::Warn("Socket {}: {}", identity, message);
                else Logger::Error("Socket {}: {}", identity, message);
            }, p_message.size());
    } catch (...) {} // diagnostics must never interrupt transport cleanup
}

void RedSocket::RegisterListener(const Red::Handle<Red::IScriptable>& p_object,
                                const Red::CName& p_onCommand,
                                const Red::CName& p_onConnection,
                                const Red::Optional<Red::CName>& p_onDisconnection) {
    (void)TryRegisterListener(p_object, p_onCommand, p_onConnection, p_onDisconnection);
}

bool RedSocket::TryRegisterListener(const Red::Handle<Red::IScriptable>& p_object,
                                   const Red::CName& p_onCommand, const Red::CName& p_onConnection,
                                   const Red::Optional<Red::CName>& p_onDisconnection) {
    if (!m_isTracked || !p_object || p_onCommand.IsNone() || p_onConnection.IsNone()) return false;
    try {
        RedCallback replacement{p_object, p_onCommand, p_onConnection, p_onDisconnection.value};
        {
            std::lock_guard lock(m_callbackMutex);
            if (IsRetired() || m_connectPending ||
                m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Disconnected ||
                m_pendingTerminalEvents.load(std::memory_order_acquire) != 0) return false;
            std::swap(m_callback, replacement);
        }
        return true; // prior reference released outside callback lock
    } catch (...) { return false; }
}

void RedSocket::UnregisterListener() {
    RedCallback released{};
    {
        std::lock_guard lock(m_callbackMutex);
        if (!m_isTracked || IsRetired() || m_connectPending ||
            m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Disconnected ||
            m_pendingTerminalEvents.load(std::memory_order_acquire) != 0) return;
        std::swap(m_callback, released);
    }
}

void RedSocket::ClearRegistration() {
    // Only external native/lifecycle callers release RED handles here.
    RedCallback released{};
    {
        std::lock_guard lock(m_callbackMutex);
        std::swap(m_callback, released);
        m_connectPending = false;
    }
}

void RedSocket::Connect(const Red::CString& p_ipAddress, uint16_t p_port) {
    if (!m_isTracked || IsRetired()) return;
    const auto length = p_ipAddress.Length();
    if (length == 0 || length > 255 || p_port == 0) return;
    const std::string_view raw(p_ipAddress.c_str(), length);
    if (!std::all_of(raw.begin(), raw.end(), [](unsigned char c) { return c > 0x20 && c < 0x7f; }))
        return;
    bool reserved = false;
    try {
        std::string address(raw);
        const auto sessionEpoch = SocketService::Get().SessionEpoch();
        {
            std::lock_guard lock(m_callbackMutex);
            if (IsRetired() || m_connectPending ||
                m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Disconnected ||
                m_pendingTerminalEvents.load(std::memory_order_acquire) != 0 ||
                !m_callback.Object || m_callback.OnCommand.IsNone() || m_callback.OnConnection.IsNone()) return;
            m_connectPending = true;
            reserved = true;
        }
        if (m_owner.Post([this, address = std::move(address), p_port, sessionEpoch]() mutable {
                StartConnect(std::move(address), p_port, sessionEpoch);
            }, length)) return;
    } catch (...) {}
    if (reserved) { std::lock_guard lock(m_callbackMutex); m_connectPending = false; }
    QueueRuntimeLog("Connect offer rejected.", 1);
}

void RedSocket::StartConnect(std::string address, uint16_t port, uint64_t sessionEpoch) {
    uint64_t generation = 0;
    {
        std::lock_guard lock(m_callbackMutex);
        m_connectPending = false;
        if (IsRetired() || m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Disconnected ||
            !SocketService::Get().IsSessionCurrent(sessionEpoch)) return;
        m_cleanEofGeneration.store(0, std::memory_order_release);
        generation = m_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
        m_connectionSessionEpoch.store(sessionEpoch, std::memory_order_release);
        m_lifecycle.store(SocketLifecycleState::Resolving, std::memory_order_release);
        m_isConnected.store(false, std::memory_order_release);
    }
    try {
        auto transport = std::make_shared<SocketTransportEpoch>(m_owner.GetExecutor());
        m_transport = transport;
#if defined(DEEPBIDE_NATIVE_TEST)
        if (m_failResolveForTest.exchange(false)) throw std::bad_alloc();
#endif
        transport->Resolver.async_resolve(address, std::to_string(port),
            asio::bind_executor(m_owner.GetExecutor(),
                [this, generation, transport](const asio::error_code& error,
                                             const asio::ip::tcp::resolver::results_type& endpoints) {
                    if (generation != m_generation.load(std::memory_order_acquire) ||
                        m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Resolving) return;
                    if (error) { FailConnect(generation, error.value()); return; }
                    m_lifecycle.store(SocketLifecycleState::Connecting, std::memory_order_release);
                    try {
                        asio::async_connect(transport->Socket, endpoints,
                            asio::bind_cancellation_slot(transport->ConnectCancel.slot(),
                                asio::bind_executor(m_owner.GetExecutor(),
                                    [this, generation, transport](const asio::error_code& connectError,
                                                                 const asio::ip::tcp::endpoint&) {
                                        if (generation != m_generation.load(std::memory_order_acquire) ||
                                            m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connecting) {
                                            transport->Close(); return;
                                        }
                                        if (connectError) { FailConnect(generation, connectError.value()); return; }
                                        m_lifecycle.store(SocketLifecycleState::Connected, std::memory_order_release);
                                        m_isConnected.store(true, std::memory_order_release);
                                        QueueRuntimeLog("Connected.", 0);
                                        DispatchConnection(generation, 0);
                                        StartRead(generation);
                                    })));
                    } catch (...) { FailConnect(generation, -1); }
                }));
    } catch (...) { FailConnect(generation, -1); }
}

void RedSocket::FailConnect(uint64_t generation, int32_t status) {
    if (generation != m_generation.load(std::memory_order_acquire)) return;
    if (auto transport = std::exchange(m_transport, {})) transport->Close();
    {
        std::lock_guard lock(m_callbackMutex);
        m_pendingTerminalEvents.fetch_add(1, std::memory_order_acq_rel);
        m_lifecycle.store(SocketLifecycleState::Disconnected, std::memory_order_release);
        m_isConnected.store(false, std::memory_order_release);
    }
    QueueRuntimeLog("Connection failed.", 1);
    DispatchConnection(generation, status != 0 ? status : -1, true);
}

void RedSocket::Disconnect() {
    if (!m_isTracked || IsRetired() || m_disconnectPending.exchange(true)) return;
    if (!m_owner.Post([this] {
            CloseOnExecutor(0, false, true);
            m_disconnectPending.store(false);
        }, 0, true)) {
        m_disconnectPending.store(false);
        SocketService::Get().Retire(this, m_eventIdentity);
    }
}

bool RedSocket::CanDrainCleanEof(uint64_t generation) const noexcept {
    if (!generation || IsRetired() || m_cleanEofGeneration.load(std::memory_order_acquire) != generation) return false;
    const auto current = m_generation.load(std::memory_order_acquire);
    return current == generation || (current - generation == 1);
}

void RedSocket::CloseOnExecutor(uint64_t expected, bool destroy, bool notifyRequested, bool cleanEof) {
    const auto current = m_generation.load(std::memory_order_acquire);
    const auto state = m_lifecycle.load(std::memory_order_acquire);
    const auto plan = PlanSocketClose(expected, current, state, destroy, notifyRequested);
    if (!plan.Apply) return;
    // Clean EOF preserves only already-admitted complete lines of this epoch.
    // An explicit close, malformed input, overload or retirement never does.
    m_cleanEofGeneration.store(cleanEof && plan.NotifyDisconnection ? current : 0, std::memory_order_release);
    m_lifecycle.store(SocketLifecycleState::Closing, std::memory_order_release);
    m_isConnected.store(false, std::memory_order_release);
    const auto closed = m_generation.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (auto transport = std::exchange(m_transport, {})) transport->Close();
    for (const auto& write : m_writeQueue) SocketService::Get().ClearSendError(write.SendErrorId);
    m_writeQueue.clear();
    m_pendingWriteBytes = 0;
    m_writeInProgress = false;
    {
        std::lock_guard lock(m_callbackMutex);
        if (plan.NotifyDisconnection) m_pendingTerminalEvents.fetch_add(1, std::memory_order_acq_rel);
        m_lifecycle.store(destroy ? SocketLifecycleState::Destroyed : SocketLifecycleState::Disconnected,
                          std::memory_order_release);
    }
    if (plan.NotifyDisconnection) {
        DispatchDisconnection(closed, true);
        QueueRuntimeLog("Disconnected.", 0);
    }
}

void RedSocket::StartRead(uint64_t generation) {
    if (generation != m_generation.load(std::memory_order_acquire) ||
        m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected) return;
    const auto transport = m_transport;
    if (!transport) return;
    try {
        transport->Socket.async_read_some(asio::buffer(transport->ReadBuffer),
            asio::bind_executor(m_owner.GetExecutor(),
                [this, generation, transport](const asio::error_code& error, std::size_t length) {
                    HandleRead(generation, transport, error, length);
                }));
    } catch (...) { CloseOnExecutor(generation, false, true); }
}

void RedSocket::HandleRead(uint64_t generation, const std::shared_ptr<SocketTransportEpoch>& transport,
                           const asio::error_code& error, std::size_t length) {
    if (generation != m_generation.load(std::memory_order_acquire) ||
        m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected) return;
    const bool eof = error == asio::error::eof;
    if (error && !eof) { CloseOnExecutor(generation, false, true); return; }
    try {
        transport->Lines.Append(transport->ReadBuffer.data(), length);
        for (;;) {
            std::string line;
            const auto result = transport->Lines.Take(line, MAX_RECEIVE_BUFFER_BYTES);
            if (result == SocketLineResult::TooLarge || result == SocketLineResult::Malformed) {
                QueueRuntimeLog("Invalid or oversized receive line.", 1);
                CloseOnExecutor(generation, false, true);
                return;
            }
            if (result == SocketLineResult::NeedMore) {
                if (eof) CloseOnExecutor(generation, false, true, true);
                else StartRead(generation);
                return;
            }
            DispatchCommand(generation, std::move(line));
            if (m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected ||
                generation != m_generation.load(std::memory_order_acquire)) return;
        }
    } catch (...) { CloseOnExecutor(generation, false, true); }
}

void RedSocket::SendCommand(const Red::CString& command,
                            const Red::Optional<int32_t, 10>& limit,
                            const Red::Optional<Red::Handle<Red::IScriptable>>& target,
                            const Red::Optional<Red::CName>& onError) {
    (void)limit; // no retry loop
    if (!m_isTracked || IsRetired()) return;
    uint64_t id = 0;
    try {
        if (target.value && !onError.value.IsNone()) {
            id = SocketService::Get().RegisterSendError(target.value, onError.value, this, m_eventIdentity);
            if (!id) return;
        }
        const auto size = command.Length();
        if (size > MAX_PENDING_WRITE_BYTES - 2 ||
            !IsSingleSocketLine(std::string_view(command.c_str(), size))) {
            SocketService::Get().QueueSendError(id, this, m_eventIdentity);
            return;
        }
        const auto generation = m_generation.load(std::memory_order_acquire);
        if (!IsConnected()) { SocketService::Get().QueueSendError(id, this, m_eventIdentity); return; }
        std::string message(command.c_str(), size);
        message += "\r\n";
        const auto bytes = message.size();
        if (m_owner.Post([this, generation, message = std::move(message), id, bytes]() mutable {
                if (generation != m_generation.load(std::memory_order_acquire) ||
                    m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected ||
                    bytes > MAX_PENDING_WRITE_BYTES - m_pendingWriteBytes ||
                    m_writeQueue.size() >= SocketWritePayload::kMaxEntries) {
                    SocketService::Get().QueueSendError(id, this, m_eventIdentity); return;
                }
                try {
                    auto payload = SocketWritePayload::Create(std::move(message));
                    if (!payload) { SocketService::Get().QueueSendError(id, this, m_eventIdentity); return; }
                    m_writeQueue.push_back(PendingWrite{std::move(payload), id});
                    m_pendingWriteBytes += bytes;
                } catch (...) { SocketService::Get().QueueSendError(id, this, m_eventIdentity); return; }
                if (!m_writeInProgress) StartWrite(generation);
            }, bytes)) return;
    } catch (...) {}
    SocketService::Get().QueueSendError(id, this, m_eventIdentity);
}

void RedSocket::StartWrite(uint64_t generation) {
    if (generation != m_generation.load(std::memory_order_acquire) ||
        m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected ||
        m_writeQueue.empty()) { m_writeInProgress = false; return; }
    m_writeInProgress = true;
    const auto transport = m_transport;
    const auto payload = m_writeQueue.front().Payload;
    try {
#if defined(DEEPBIDE_NATIVE_TEST)
        if (m_failWriteForTest.exchange(false)) throw std::bad_alloc();
#endif
        asio::async_write(transport->Socket, asio::buffer(payload->Text),
            asio::bind_cancellation_slot(transport->WriteCancel.slot(),
                asio::bind_executor(m_owner.GetExecutor(),
                    RetainSocketBuffer(payload, [this, generation, transport](const auto&,
                        const asio::error_code& error, std::size_t length) {
                            HandleWrite(generation, error, length);
                        }))));
    } catch (...) {
        HandleWrite(generation, asio::error::operation_aborted, 0);
    }
}

void RedSocket::HandleWrite(uint64_t generation, const asio::error_code& error, std::size_t length) {
    if (generation != m_generation.load(std::memory_order_acquire) ||
        m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Connected) return;
    m_writeInProgress = false;
    if (m_writeQueue.empty()) { CloseOnExecutor(generation, false, true); return; }
    const auto bytes = m_writeQueue.front().Payload->Text.size();
    const auto id = m_writeQueue.front().SendErrorId;
    // Remove exactly this disposition before close invalidates the rest.
    m_pendingWriteBytes -= std::min(m_pendingWriteBytes, bytes);
    m_writeQueue.pop_front();
    if (error || length != bytes) {
        SocketService::Get().QueueSendError(id, this, m_eventIdentity);
        CloseOnExecutor(generation, false, true);
        return;
    }
    SocketService::Get().ClearSendError(id);
    StartWrite(generation);
}

void RedSocket::DispatchConnection(uint64_t p_generation, int32_t p_status, bool alreadyReserved) {
    const auto sessionEpoch = m_connectionSessionEpoch.load(std::memory_order_acquire);
    if (!alreadyReserved) m_pendingTerminalEvents.fetch_add(1, std::memory_order_acq_rel);
    const auto identity = m_eventIdentity;
    bool queued = false;
    try { queued = SocketService::Get().QueueEvent(this, identity, [p_status, p_generation, sessionEpoch, identity](const Red::Handle<RedSocket>& self) {
        if (!self) return;
        TerminalCallbackGuard completion(self->m_pendingTerminalEvents);
        const auto state = self->m_lifecycle.load(std::memory_order_acquire);
        const bool currentSession = SocketService::Get().IsSessionCurrent(sessionEpoch);
        const bool deliver = self->GetEventIdentity() == identity && currentSession &&
            (ShouldDeliverSocketCallback(SocketCallbackKind::Connection, p_generation,
                self->m_generation.load(std::memory_order_acquire), state,
                state == SocketLifecycleState::Destroyed, currentSession) ||
                (p_status == 0 && self->CanDrainCleanEof(p_generation)));
        if (!deliver) return;
        RedCallback callback;
        { std::lock_guard lock(self->m_callbackMutex); callback = self->m_callback; }
        completion.Release();
        if (callback.Object && !self->IsRetired() && !callback.OnConnection.IsNone())
            Red::CallVirtual(callback.Object.GetPtr(), callback.OnConnection, p_status);
    }, 0, true); } catch (...) {}
    if (!queued) m_pendingTerminalEvents.fetch_sub(1, std::memory_order_acq_rel);
}

void RedSocket::DispatchDisconnection(uint64_t p_generation, bool alreadyReserved) {
    const auto sessionEpoch = m_connectionSessionEpoch.load(std::memory_order_acquire);
    if (!alreadyReserved) m_pendingTerminalEvents.fetch_add(1, std::memory_order_acq_rel);
    const auto identity = m_eventIdentity;
    bool queued = false;
    try { queued = SocketService::Get().QueueEvent(this, identity, [p_generation, sessionEpoch, identity](const Red::Handle<RedSocket>& self) {
        if (!self) return;
        TerminalCallbackGuard completion(self->m_pendingTerminalEvents);
        const auto state = self->m_lifecycle.load(std::memory_order_acquire);
        const bool deliver = self->GetEventIdentity() == identity &&
            ShouldDeliverSocketCallback(SocketCallbackKind::Disconnection, p_generation,
                self->m_generation.load(std::memory_order_acquire), state,
                state == SocketLifecycleState::Destroyed,
                SocketService::Get().IsSessionCurrent(sessionEpoch));
        if (!deliver) return;
        RedCallback callback;
        { std::lock_guard lock(self->m_callbackMutex); callback = self->m_callback; }
        self->m_cleanEofGeneration.store(0, std::memory_order_release);
        completion.Release();
        if (callback.Object && !self->IsRetired() && !callback.OnDisconnection.IsNone())
            Red::CallVirtual(callback.Object.GetPtr(), callback.OnDisconnection);
    }, 0, true); } catch (...) {}
    if (!queued) m_pendingTerminalEvents.fetch_sub(1, std::memory_order_acq_rel);
}

void RedSocket::DispatchCommand(uint64_t p_generation, std::string p_command) {
    const auto accountedBytes = p_command.size();
    const auto sessionEpoch = m_connectionSessionEpoch.load(std::memory_order_acquire);
    const auto identity = m_eventIdentity;
    const bool queued = SocketService::Get().QueueEvent(this, identity, [p_generation, sessionEpoch, identity, command = std::move(p_command)](const Red::Handle<RedSocket>& self) {
        if (!self) return;
        if (self->GetEventIdentity() != identity ||
            !SocketService::Get().IsSessionCurrent(sessionEpoch)) return;
        const bool live = self->m_generation.load(std::memory_order_acquire) == p_generation &&
            self->m_lifecycle.load(std::memory_order_acquire) == SocketLifecycleState::Connected;
        if (!live && !self->CanDrainCleanEof(p_generation)) return;
        Red::CString redCommand(command.data(), static_cast<uint32_t>(command.size()));
        RedCallback callback;
        { std::lock_guard lock(self->m_callbackMutex); callback = self->m_callback; }
        if (callback.Object && !self->IsRetired() && !callback.OnCommand.IsNone())
            Red::CallVirtual(callback.Object.GetPtr(), callback.OnCommand, redCommand);
    }, accountedBytes);
    if (!queued) {
        CloseOnExecutor(p_generation, false, true);
    }
}

void RedSocket::Run() {
}

void RedSocket::Poll() {
}

void RedSocket::Shutdown() {
    std::lock_guard external(m_shutdownMutex);
    m_shutdownStarted.store(true, std::memory_order_release);
    if (!m_owner.Stop([this] { CloseOnExecutor(0, true, false); })) {
        if (!m_owner.ReapStoppedWorker()) std::terminate();
    }
    // Posting/finalization can fail. After reaping the worker, invalidate
    // the epoch and close the transport before draining aborted completions.
    if (m_lifecycle.load(std::memory_order_acquire) != SocketLifecycleState::Destroyed) {
        if (auto transport = std::exchange(m_transport, {})) transport->Close();
        for (const auto& write : m_writeQueue) SocketService::Get().ClearSendError(write.SendErrorId);
        m_writeQueue.clear();
        m_pendingWriteBytes = 0;
        m_writeInProgress = false;
        m_generation.fetch_add(1, std::memory_order_acq_rel);
        m_isConnected.store(false, std::memory_order_release);
        m_lifecycle.store(SocketLifecycleState::Destroyed, std::memory_order_release);
    }
    // A stopped io_context otherwise retains pending handler captures until
    // the last external socket reference disappears. Discard queued commands
    // and drain only stale/canceled completions after epoch invalidation.
    if (!m_owner.DrainAbortedAfterJoin()) std::terminate();
}
