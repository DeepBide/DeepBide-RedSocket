#include "SocketOwner.h"
#include "SocketGeneration.h"
#include "SocketEventBudget.h"
#include "SocketSessionGate.h"
#include "SocketCallbackGate.h"

#include <asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <list>
#include <future>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace std::chrono_literals;
using asio::ip::tcp;

class LoopbackServer {
    struct Session : std::enable_shared_from_this<Session> {
        tcp::socket socket;
        asio::streambuf input;
        asio::steady_timer resetTimer;
        bool telemetry;
        int hudLines{0};

        Session(tcp::socket p_socket, bool p_telemetry)
            : socket(std::move(p_socket)), resetTimer(socket.get_executor()), telemetry(p_telemetry) {}

        void Read() {
            auto self = shared_from_this();
            asio::async_read_until(socket, input, "\r\n", [self](const asio::error_code& error, std::size_t) {
                if (error) return;
                std::istream stream(&self->input);
                std::string line;
                std::getline(stream, line);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                const auto reply = self->telemetry ? "ACK|TELEMETRY\r\n" :
                    (self->hudLines++ == 0 ? "RECEIVED|SHOW|h1\r\n" : "RECEIVED|HIDE|h1\r\n");
                auto payload = std::make_shared<std::string>(reply);
                asio::async_write(self->socket, asio::buffer(*payload),
                    [self, payload](const asio::error_code& writeError, std::size_t) {
                        if (writeError) return;
                        if (self->telemetry) {
                            self->resetTimer.expires_after(250ms);
                            self->resetTimer.async_wait([self](const asio::error_code& timerError) {
                                if (timerError) return;
                                asio::error_code ignored;
                                self->socket.set_option(asio::socket_base::linger(true, 0), ignored);
                                self->socket.close(ignored);
                            });
                        } else {
                            self->Read();
                        }
                    });
            });
        }
    };

    asio::io_context m_context;
    tcp::acceptor m_telemetry{m_context};
    tcp::acceptor m_hud{m_context};
    std::thread m_thread;
    uint16_t m_telemetryPort{}, m_hudPort{};

    void Accept(tcp::acceptor& p_acceptor, bool p_telemetry) {
        p_acceptor.async_accept([this, &p_acceptor, p_telemetry](const asio::error_code& error, tcp::socket socket) {
            if (!error) std::make_shared<Session>(std::move(socket), p_telemetry)->Read();
            if (p_acceptor.is_open()) Accept(p_acceptor, p_telemetry);
        });
    }

public:
    explicit LoopbackServer(bool p_telemetryHostPresent = true) {
        const auto loopback = asio::ip::make_address("127.0.0.1");
        // Ephemeral reserved ports never collide with installed consumers.
        // A bound but non-listening telemetry endpoint models refusal.
        m_telemetry.open(tcp::v4());
        m_telemetry.bind({loopback, 0});
        m_telemetryPort = m_telemetry.local_endpoint().port();
        if (p_telemetryHostPresent) m_telemetry.listen();
        m_hud.open(tcp::v4());
        m_hud.bind({loopback, 0});
        m_hudPort = m_hud.local_endpoint().port();
        m_hud.listen();
        if (p_telemetryHostPresent) Accept(m_telemetry, true);
        Accept(m_hud, false);
        m_thread = std::thread([this] { m_context.run(); });
    }

    uint16_t TelemetryPort() const { return m_telemetryPort; }
    uint16_t HudPort() const { return m_hudPort; }

    ~LoopbackServer() {
        // Stop and join before touching acceptors used by the context thread.
        m_context.stop();
        if (m_thread.joinable()) m_thread.join();
        asio::error_code ignored;
        m_telemetry.close(ignored);
        m_hud.close(ignored);
    }
};

class ClientChannel {
    SocketOwner m_owner;
    tcp::resolver m_resolver{m_owner.GetExecutor()};
    tcp::socket m_socket{m_owner.GetExecutor()};
    asio::streambuf m_input;
    std::thread::id m_ownerThread{};
    std::atomic_bool m_affinityOk{true};

    void MarkOwner() {
        const auto current = std::this_thread::get_id();
        if (m_ownerThread == std::thread::id{}) m_ownerThread = current;
        else if (m_ownerThread != current) m_affinityOk.store(false);
    }

public:
    ~ClientChannel() { Stop(); }
    std::future<std::string> SendAndRead(uint16_t p_port, std::string p_message) {
        auto result = std::make_shared<std::promise<std::string>>();
        auto future = result->get_future();
        const bool posted = m_owner.Post([this, p_port, p_message = std::move(p_message), result] {
            MarkOwner();
            m_resolver.async_resolve("127.0.0.1", std::to_string(p_port),
                asio::bind_executor(m_owner.GetExecutor(), [this, p_message, result](const asio::error_code& error, tcp::resolver::results_type endpoints) {
                    MarkOwner();
                    if (error) { result->set_exception(std::make_exception_ptr(std::runtime_error(error.message()))); return; }
                    asio::async_connect(m_socket, endpoints,
                        asio::bind_executor(m_owner.GetExecutor(), [this, p_message, result](const asio::error_code& connectError, const tcp::endpoint&) {
                            MarkOwner();
                            if (connectError) { result->set_exception(std::make_exception_ptr(std::runtime_error(connectError.message()))); return; }
                            auto payload = std::make_shared<std::string>(p_message + "\r\n");
                            asio::async_write(m_socket, asio::buffer(*payload), asio::bind_executor(m_owner.GetExecutor(),
                                [this, payload, result](const asio::error_code& writeError, std::size_t) {
                                    MarkOwner();
                                    if (writeError) { result->set_exception(std::make_exception_ptr(std::runtime_error(writeError.message()))); return; }
                                    asio::async_read_until(m_socket, m_input, "\r\n", asio::bind_executor(m_owner.GetExecutor(),
                                        [this, result](const asio::error_code& readError, std::size_t) {
                                            MarkOwner();
                                            if (readError) { result->set_exception(std::make_exception_ptr(std::runtime_error(readError.message()))); return; }
                                            std::istream stream(&m_input);
                                            std::string line;
                                            std::getline(stream, line);
                                            if (!line.empty() && line.back() == '\r') line.pop_back();
                                            result->set_value(line);
                                        }));
                                }));
                        }));
                }));
        });
        if (!posted) result->set_exception(std::make_exception_ptr(std::runtime_error("owner stopped")));
        return future;
    }

    std::future<bool> WaitForDisconnect() {
        auto result = std::make_shared<std::promise<bool>>();
        auto future = result->get_future();
        m_owner.Post([this, result] {
            MarkOwner();
            auto buffer = std::make_shared<std::array<char, 16>>();
            m_socket.async_read_some(asio::buffer(*buffer), asio::bind_executor(m_owner.GetExecutor(),
                [this, result, buffer](const asio::error_code& error, std::size_t) {
                    MarkOwner();
                    result->set_value(static_cast<bool>(error));
                }));
        });
        return future;
    }

    std::future<bool> ArmPendingRead() {
        auto armed = std::make_shared<std::promise<bool>>();
        auto armedFuture = armed->get_future();
        m_owner.Post([this, armed] {
            MarkOwner();
            auto buffer = std::make_shared<std::array<char, 16>>();
            m_socket.async_read_some(asio::buffer(*buffer), asio::bind_executor(m_owner.GetExecutor(),
                [this, armed, buffer](const asio::error_code& error, std::size_t) {
                    MarkOwner();
                    armed->set_value(static_cast<bool>(error));
                }));
        });
        return armedFuture;
    }

    std::future<std::string> SendAgain(std::string p_message) {
        auto result = std::make_shared<std::promise<std::string>>();
        auto future = result->get_future();
        m_owner.Post([this, p_message = std::move(p_message), result] {
            MarkOwner();
            auto payload = std::make_shared<std::string>(p_message + "\r\n");
            asio::async_write(m_socket, asio::buffer(*payload), asio::bind_executor(m_owner.GetExecutor(),
                [this, payload, result](const asio::error_code& error, std::size_t) {
                    MarkOwner();
                    if (error) { result->set_exception(std::make_exception_ptr(std::runtime_error(error.message()))); return; }
                    asio::async_read_until(m_socket, m_input, "\r\n", asio::bind_executor(m_owner.GetExecutor(),
                        [this, result](const asio::error_code& readError, std::size_t) {
                            MarkOwner();
                            if (readError) { result->set_exception(std::make_exception_ptr(std::runtime_error(readError.message()))); return; }
                            std::istream stream(&m_input);
                            std::string line;
                            std::getline(stream, line);
                            if (!line.empty() && line.back() == '\r') line.pop_back();
                            result->set_value(line);
                        }));
                }));
        });
        return future;
    }

    // Reproduce the installed CET telemetry producer's repeated connect attempts
    // on 20770 when no receiver is listening. This socket is independent from
    // the HUD's existing 20771 connection. No game or RED4ext is involved.
    std::future<bool> ProbeConnectionRefused(uint16_t p_port) {
        auto result = std::make_shared<std::promise<bool>>();
        auto future = result->get_future();
        const bool posted = m_owner.Post([this, p_port, result] {
            MarkOwner();
            asio::error_code ignored;
            m_socket.cancel(ignored);
            m_socket.close(ignored);
            m_socket.open(tcp::v4(), ignored);
            if (ignored) {
                result->set_exception(std::make_exception_ptr(
                    std::runtime_error("cannot reopen telemetry socket")));
                return;
            }
            m_socket.async_connect({asio::ip::make_address("127.0.0.1"), p_port},
                asio::bind_executor(m_owner.GetExecutor(), [this, result](const asio::error_code& error) {
                    MarkOwner();
                    result->set_value(error == asio::error::connection_refused);
                }));
        });
        if (!posted) {
            result->set_exception(std::make_exception_ptr(std::runtime_error("owner stopped")));
        }
        return future;
    }

    bool AffinityOk() const { return m_affinityOk.load(); }

    void Stop() {
        if (!m_owner.Stop([this] {
            asio::error_code ignored;
            m_socket.cancel(ignored);
            m_socket.close(ignored);
            m_resolver.cancel();
        })) m_owner.ReapStoppedWorker();
    }

    std::future<bool> StopFromOwner() {
        auto result = std::make_shared<std::promise<bool>>();
        auto future = result->get_future();
        if (!m_owner.Post([this, result] { result->set_value(m_owner.Stop([] {})); }))
            throw std::runtime_error("could not schedule owner-thread Stop regression");
        return future;
    }
};

// Regression for the physical #121 trace: telemetry 20770 can repeatedly
// fail while the HUD 20771 is valid. In particular, failed connects in a
// shared RedSocket log cannot be attributed to the HUD without socket identity.
void TestRefusedTelemetryWhileHudAlive() {
    LoopbackServer hudOnly(false);
    ClientChannel hud;
    ClientChannel telemetry;
    auto hudShow = hud.SendAndRead(hudOnly.HudPort(), "SHOW|h1");
    if (hudShow.wait_for(5s) != std::future_status::ready ||
        hudShow.get() != "RECEIVED|SHOW|h1") {
        throw std::runtime_error("HUD failed despite healthy 20771 listener");
    }
    for (int index = 0; index < 10; ++index) {
        auto failed = telemetry.ProbeConnectionRefused(hudOnly.TelemetryPort());
        if (failed.wait_for(5s) != std::future_status::ready || !failed.get()) {
            throw std::runtime_error("20770 did not refuse as expected with HUD alive");
        }
        if (index == 4) {
            auto hudHide = hud.SendAgain("HIDE|h1");
            if (hudHide.wait_for(5s) != std::future_status::ready ||
                hudHide.get() != "RECEIVED|HIDE|h1") {
                throw std::runtime_error("telemetry failures disrupted HUD HIDE");
            }
        }
    }
    if (!telemetry.AffinityOk() || !hud.AffinityOk()) {
        throw std::runtime_error("socket owner affinity violated during failed connect mix");
    }
    telemetry.Stop();
    auto cancelRead = hud.ArmPendingRead();
    hud.Stop();
    if (cancelRead.wait_for(5s) != std::future_status::ready || !cancelRead.get()) {
        throw std::runtime_error("HUD pending read did not cancel during teardown");
    }
}

class LifecycleProbe {
    SocketOwner m_owner;
    uint64_t m_generation{0};
    bool m_connected{false};
    int m_closeCount{0};
    int m_callbackCount{0};

public:
    uint64_t Connect() {
        auto result = std::make_shared<std::promise<uint64_t>>();
        auto future = result->get_future();
        m_owner.Post([this, result] {
            m_generation++;
            m_connected = true;
            result->set_value(m_generation);
        });
        return future.get();
    }

    void CloseIfGeneration(uint64_t p_generation) {
        m_owner.Post([this, p_generation] {
            if (!m_connected || !IsCurrentSocketGeneration(p_generation, m_generation)) return;
            m_connected = false;
            ++m_closeCount;
            ++m_callbackCount;
            // Model a reentrant duplicate close request. It is queued on the
            // same owner and observes the completed state without deadlock.
            m_owner.Post([this, p_generation] {
                if (m_connected && IsCurrentSocketGeneration(p_generation, m_generation)) {
                    m_connected = false;
                    ++m_closeCount;
                    ++m_callbackCount;
                }
            });
        });
    }

    std::array<int, 3> Snapshot() {
        auto result = std::make_shared<std::promise<std::array<int, 3>>>();
        auto future = result->get_future();
        m_owner.Post([this, result] {
            result->set_value({m_connected ? 1 : 0, m_closeCount, m_callbackCount});
        });
        return future.get();
    }

    void Stop() { m_owner.Stop([] {}); }
};

void TestProductionClosePlan() {
    auto close = PlanSocketClose(5, 5, SocketLifecycleState::Connected, false, true);
    if (!close.Apply || !close.NotifyDisconnection) throw std::runtime_error("connected close was not eligible exactly once");
    const uint64_t closedGeneration = 6;
    close = PlanSocketClose(0, closedGeneration, SocketLifecycleState::Disconnected, false, true);
    if (close.Apply || close.NotifyDisconnection) throw std::runtime_error("duplicate close advanced a disconnected generation");
    close = PlanSocketClose(5, 7, SocketLifecycleState::Connected, false, true);
    if (close.Apply) throw std::runtime_error("stale generation was allowed to close a later connection");
    close = PlanSocketClose(8, 8, SocketLifecycleState::Resolving, false, true);
    if (!close.Apply || close.NotifyDisconnection) throw std::runtime_error("disconnect during resolution was not applied without a false callback");
    close = PlanSocketClose(9, 9, SocketLifecycleState::Connecting, false, true);
    if (!close.Apply || close.NotifyDisconnection) throw std::runtime_error("disconnect during connect was not applied without a false callback");
    close = PlanSocketClose(0, 7, SocketLifecycleState::Disconnected, true, true);
    if (!close.Apply || close.NotifyDisconnection) throw std::runtime_error("destroy of disconnected object produced a disconnect callback");
    close = PlanSocketClose(0, 7, SocketLifecycleState::Closing, false, true);
    if (close.Apply) throw std::runtime_error("reentrant close while Closing was applied twice");
}

void TestProductionEventBudget() {
    SocketEventBudget budget;
    for (std::size_t index = 0; index < SocketEventBudget::kMaxEventsPerSocket; ++index) {
        if (!budget.TryReserve(1, 512, false)) throw std::runtime_error("first channel exhausted before its quota");
    }
    if (budget.TryReserve(1, 512, false)) throw std::runtime_error("first channel exceeded its event quota");
    if (!budget.TryReserve(2, 512, false)) throw std::runtime_error("first channel starved the second channel");
    budget.Release(1, 512, false);
    if (!budget.TryReserve(1, 512, false)) throw std::runtime_error("released event quota was not reusable");
    for (std::size_t index = 0; index < SocketEventBudget::kMaxCriticalPerSocket; ++index)
        if (!budget.TryReserve(1, 512, true)) throw std::runtime_error("critical identity reserve too small");
    if (budget.TryReserve(1, 512, true) || !budget.TryReserve(2, 512, true))
        throw std::runtime_error("critical overload crossed socket ownership");
    budget.Release(1, 512, true);
    if (!budget.TryReserve(1, 512, true)) throw std::runtime_error("critical reserve not reusable");
    budget.Reset();
    if (budget.EventCount() || budget.ByteCount() || budget.CriticalCount())
        throw std::runtime_error("event budget did not recover after pump drain");
}

void TestProductionEventDispatchBudgetAndFairness() {
    using Budget = SocketEventDispatchBudget;
    using Scheduler = SocketEventDispatchScheduler;
    using namespace std::chrono_literals;
    if (!Budget::CanDispatchNext(0, 0, 512, 10ms))
        throw std::runtime_error("empty update did not guarantee progress for the first event");
    if (!Budget::CanDispatchNext(Budget::kMaxEventsPerUpdate - 1,
            (Budget::kMaxEventsPerUpdate - 1) * 512, 512, 1ms))
        throw std::runtime_error("event-count budget ended before its bound");
    if (Budget::CanDispatchNext(Budget::kMaxEventsPerUpdate, 0, 512, 0ms))
        throw std::runtime_error("event-count budget was exceeded");
    if (Budget::CanDispatchNext(1, Budget::kMaxBytesPerUpdate - 128, 256, 0ms))
        throw std::runtime_error("byte budget was exceeded");
    if (Budget::CanDispatchNext(1, 512, 512, Budget::kSoftTimePerUpdate))
        throw std::runtime_error("soft time budget did not stop between callbacks");
    if (Budget::CanDispatchNext(0, 0, Budget::kMaxBytesPerUpdate + 1, 0ms))
        throw std::runtime_error("oversized event bypassed the per-update byte bound");

    struct Event {
        uint64_t Identity;
        std::size_t AccountedBytes;
        bool Critical;
        int Lane;
        int Generation;
        int Order;
    };
    std::list<Event> queue;
    for (std::size_t index = 0; index < SocketEventBudget::kMaxEventsPerSocket; ++index)
        queue.push_back({1, 512, false, 20770, 1, static_cast<int>(index)});
    queue.push_back({2, 512, true, 20771, 1, -1}); // current connection transition
    for (std::size_t index = 0; index < SocketEventBudget::kMaxEventsPerSocket; ++index)
        queue.push_back({2, 512, false, 20771, 1, static_cast<int>(index)});
    queue.push_back({1, 512, true, 20770, 1, -2}); // terminal after that socket's commands

    std::size_t updates = 0;
    std::size_t delivered = 0;
    std::size_t firstHudUpdate = 0;
    std::size_t hudConnectionUpdate = 0;
    std::size_t telemetryTerminalUpdate = 0;
    std::size_t criticalStreak = 0;
    std::size_t telemetryCommandsDelivered = 0;
    std::size_t hudCommandsDelivered = 0;
    std::size_t criticalDelivered = 0;
    while (!queue.empty()) {
        ++updates;
        std::size_t count = 0;
        std::size_t bytes = 0;
        auto elapsed = 0us;
        while (!queue.empty()) {
            auto next = Scheduler::SelectNext(queue, count, bytes, elapsed, criticalStreak);
            if (next == queue.end()) break;
            const auto event = *next;
            queue.erase(next);
            if (event.Lane == 20771 && event.Critical) hudConnectionUpdate = updates;
            if (event.Lane == 20771 && !event.Critical) {
                if (firstHudUpdate == 0) firstHudUpdate = updates;
                ++hudCommandsDelivered;
            }
            if (event.Lane == 20770 && !event.Critical) ++telemetryCommandsDelivered;
            if (event.Lane == 20770 && event.Critical) telemetryTerminalUpdate = updates;
            if (event.Critical) {
                ++criticalDelivered;
                criticalStreak = std::min(criticalStreak + 1,
                    Scheduler::kMaxConsecutiveCritical);
            } else {
                criticalStreak = 0;
            }
            ++count;
            ++delivered;
            bytes += event.AccountedBytes;
            elapsed += 10us; // deterministic virtual clock; no wall-clock sleep
        }
        if (count == 0) throw std::runtime_error("bounded production scheduler made no progress");
    }
    const auto expectedCount = 2 * SocketEventBudget::kMaxEventsPerSocket +
        2;
    const auto expectedHudUpdate = SocketEventBudget::kMaxEventsPerSocket /
        Budget::kMaxEventsPerUpdate + 1;
    const auto maximumUpdates = (expectedCount + Budget::kMaxEventsPerUpdate - 1) /
        Budget::kMaxEventsPerUpdate;
    const auto legacyFifoCriticalUpdate = SocketEventBudget::kMaxEventsPerSocket /
        Budget::kMaxEventsPerUpdate + 1;
    if (delivered != expectedCount || criticalDelivered != 2 ||
        telemetryCommandsDelivered != SocketEventBudget::kMaxEventsPerSocket ||
        hudCommandsDelivered != SocketEventBudget::kMaxEventsPerSocket)
        throw std::runtime_error("scheduler lost a normal or critical event");
    if (legacyFifoCriticalUpdate != 33 || hudConnectionUpdate != 1)
        throw std::runtime_error("R9 FIFO delay was not reduced from Update 33 to Update 1");
    if (telemetryTerminalUpdate < expectedHudUpdate - 1)
        throw std::runtime_error("terminal callback overtook earlier commands on its own socket");
    if (firstHudUpdate != expectedHudUpdate || updates > maximumUpdates)
        throw std::runtime_error("telemetry/HUD queues did not make bounded progress");

    std::list<Event> causal;
    causal.push_back({42, 512, true, 20770, 7, 1}); // connect N
    causal.push_back({42, 512, false, 20770, 7, 2}); // command N
    causal.push_back({42, 512, true, 20770, 7, 3}); // send error N
    causal.push_back({42, 512, true, 20770, 7, 4}); // disconnect N
    causal.push_back({42, 512, true, 20770, 8, 5}); // connect N+1
    causal.push_back({42, 512, false, 20770, 8, 6}); // command N+1
    const int expectedOrder[] = {1, 2, 3, 4, 5, 6};
    const int expectedGeneration[] = {7, 7, 7, 7, 8, 8};
    std::size_t causalIndex = 0;
    while (!causal.empty()) {
        auto next = Scheduler::SelectNext(causal, 0, 0, 0us, 0);
        if (next == causal.end() || next->Order != expectedOrder[causalIndex] ||
            next->Generation != expectedGeneration[causalIndex])
            throw std::runtime_error("scheduler changed per-socket generation/terminal causal order");
        ++causalIndex;
        causal.erase(next);
    }

    std::list<Event> criticalStorm;
    for (std::size_t index = 0; index < SocketEventBudget::kMaxCriticalEvents; ++index)
        criticalStorm.push_back({9, 512, true, 20770, 1, static_cast<int>(index)});
    criticalStorm.push_back({10, 512, false, 20771, 1, 999});
    std::size_t stormStreak = 0;
    std::size_t stormCalls = 0;
    std::size_t stormUpdates = 0;
    std::size_t firstNormalCall = 0;
    while (!criticalStorm.empty()) {
        ++stormUpdates;
        std::size_t updateCalls = 0;
        std::size_t updateBytes = 0;
        auto elapsed = 0us;
        while (!criticalStorm.empty()) {
            auto next = Scheduler::SelectNext(
                criticalStorm, updateCalls, updateBytes, elapsed, stormStreak);
            if (next == criticalStorm.end()) break;
            if (!next->Critical && firstNormalCall == 0) firstNormalCall = stormCalls + 1;
            if (next->Critical) {
                ++stormStreak;
            } else {
                stormStreak = 0;
            }
            criticalStorm.erase(next);
            ++stormCalls;
            ++updateCalls;
            updateBytes += 512;
            elapsed += 10us;
        }
        if (updateCalls == 0) throw std::runtime_error("critical storm scheduler stalled");
    }
    if (firstNormalCall == 0 || firstNormalCall > Scheduler::kMaxConsecutiveCritical + 1 ||
        stormUpdates > 5)
        throw std::runtime_error("bounded critical storm starved normal channel or exceeded its update bound");
}

int RunSelectorBoundednessCases() {
    using Scheduler = SocketEventDispatchScheduler;
    using namespace std::chrono_literals;
    struct Event { uint64_t Identity; std::size_t AccountedBytes; bool Critical; };

    for (std::size_t criticalCount : {std::size_t{255}, std::size_t{256}}) {
        std::list<Event> candidates;
        for (std::size_t index = 1; index <= criticalCount; ++index)
            candidates.push_back({static_cast<uint64_t>(index * 512), 512, true});
        candidates.push_back({static_cast<uint64_t>((criticalCount + 1) * 512), 512, false});
        Scheduler::SelectionDiagnostics diagnostics;
        auto selected = Scheduler::SelectNext(candidates, 0, 0, 0us, 0, &diagnostics);
        if (selected == candidates.end() || !selected->Critical || diagnostics.CapacityExceeded)
            throw std::runtime_error("critical identity boundary plus normal event did not select within bound");
        if (diagnostics.IdentityProbeSteps > (criticalCount + 1) * (criticalCount + 2) / 2)
            throw std::runtime_error("collision probing exceeded its deterministic triangular bound");
    }

    std::list<Event> maximumQueue;
    SocketEventBudget budget;
    for (std::size_t identityIndex = 1; identityIndex <= SocketEventBudget::kMaxSockets; ++identityIndex) {
        const auto identity = static_cast<uint64_t>(identityIndex);
        for (std::size_t eventIndex = 0; eventIndex < SocketEventBudget::kMaxEvents / SocketEventBudget::kMaxSockets; ++eventIndex) {
            if (!budget.TryReserve(identity, 512, false))
                throw std::runtime_error("normal load fixture exceeded the production event budget early");
            maximumQueue.push_back({identity, 512, false});
        }
    }
    for (std::size_t identityIndex = 1; identityIndex <= SocketEventBudget::kMaxSockets; ++identityIndex) {
        for (std::size_t i = 0; i < SocketEventBudget::kMaxCriticalPerSocket; ++i) {
            const auto identity = static_cast<uint64_t>(identityIndex);
            if (!budget.TryReserve(identity, 512, true))
                throw std::runtime_error("critical load fixture exceeded the production reserve early");
            maximumQueue.push_front({identity, 512, true});
        }
    }
    if (maximumQueue.size() != Scheduler::kMaxPendingEvents ||
        budget.EventCount() != SocketEventBudget::kMaxEvents ||
        budget.CriticalCount() != SocketEventBudget::kMaxCriticalEvents ||
        Scheduler::kMaxDistinctPendingIdentities != SocketEventBudget::kMaxSockets + SocketEventBudget::kMaxCriticalEvents)
        throw std::runtime_error("maximum selector fixture does not match admitted production bounds");

    std::mutex selectorMutex; // test-only contention proxy; not RED4ext's runtime lock
    std::size_t criticalStreak = 0;
    std::size_t selectedCount = 0;
    std::size_t scannedEvents = 0;
    std::size_t probeSteps = 0;
    std::size_t maxProbeSteps = 0;
    bool normalSelectedAfterFairnessBound = false;
    const auto wallStarted = std::chrono::steady_clock::now();
    while (selectedCount < SocketEventDispatchBudget::kMaxEventsPerUpdate) {
        Scheduler::SelectionDiagnostics diagnostics;
        auto selected = maximumQueue.end();
        {
            std::lock_guard lock(selectorMutex);
            selected = Scheduler::SelectNext(maximumQueue, selectedCount,
                selectedCount * std::size_t{512}, std::chrono::microseconds(selectedCount * 10),
                criticalStreak, &diagnostics);
        }
        if (selected == maximumQueue.end() || diagnostics.CapacityExceeded)
            throw std::runtime_error("maximum admitted selector load stalled or exceeded its identity bound");
        if (!selected->Critical && criticalStreak == Scheduler::kMaxConsecutiveCritical)
            normalSelectedAfterFairnessBound = true;
        criticalStreak = selected->Critical
            ? std::min(criticalStreak + 1, Scheduler::kMaxConsecutiveCritical) : 0;
        maximumQueue.erase(selected);
        ++selectedCount;
        scannedEvents += diagnostics.EventsScanned;
        probeSteps += diagnostics.IdentityProbeSteps;
        maxProbeSteps = std::max(maxProbeSteps, diagnostics.IdentityProbeSteps);
    }
    const auto wallElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - wallStarted).count();
    if (!normalSelectedAfterFairnessBound || selectedCount != SocketEventDispatchBudget::kMaxEventsPerUpdate)
        throw std::runtime_error("maximum-load selector violated its critical fairness bound");

    std::list<Event> maximumCollisionQueue;
    for (std::size_t index = 1; index <= Scheduler::kMaxCriticalIdentities; ++index)
        maximumCollisionQueue.push_back({static_cast<uint64_t>(index * Scheduler::kIdentitySetSlots), 512, true});
    for (std::size_t index = Scheduler::kMaxCriticalIdentities + 1;
         index <= Scheduler::kMaxDistinctPendingIdentities; ++index)
        maximumCollisionQueue.push_back({static_cast<uint64_t>(index * Scheduler::kIdentitySetSlots), 512, false});
    Scheduler::SelectionDiagnostics maximumCollisionDiagnostics;
    const auto collisionStarted = std::chrono::steady_clock::now();
    const auto collisionSelection = Scheduler::SelectNext(
        maximumCollisionQueue, 0, 0, 0us, 0, &maximumCollisionDiagnostics);
    const auto collisionWallUs = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - collisionStarted).count();
    const auto expectedCollisionProbes = (Scheduler::kMaxCriticalIdentities + 1) *
        (Scheduler::kMaxCriticalIdentities + 2) / 2;
    if (collisionSelection == maximumCollisionQueue.end() || maximumCollisionDiagnostics.CapacityExceeded ||
        maximumCollisionDiagnostics.IdentityProbeSteps != expectedCollisionProbes)
        throw std::runtime_error("maximum admitted collision set did not terminate with bounded probing");
    std::cout << "SELECTOR_MAX_COLLISION identities=" << Scheduler::kMaxDistinctPendingIdentities
              << " probes=" << maximumCollisionDiagnostics.IdentityProbeSteps
              << " wall_us=" << collisionWallUs << "\n";
    Scheduler::SelectionDiagnostics exhausted;
    if (Scheduler::SelectNext(maximumCollisionQueue, SocketEventDispatchBudget::kMaxEventsPerUpdate,
            0, 0us, 0, &exhausted) != maximumCollisionQueue.end() || exhausted.EventsScanned != 0)
        throw std::runtime_error("exhausted event budget scanned entries");
    std::list<Event> overIdentityCapacity;
    for (std::size_t index = 0; index < Scheduler::kIdentitySetSlots + 1; ++index)
        overIdentityCapacity.push_back({static_cast<uint64_t>((index + 1) * Scheduler::kIdentitySetSlots), 512, true});
    const auto originalSize = overIdentityCapacity.size();
    Scheduler::SelectionDiagnostics overCapacityDiagnostics;
    const auto rejected = Scheduler::SelectNext(overIdentityCapacity, 0, 0, 0us, 0, &overCapacityDiagnostics);
    if (rejected != overIdentityCapacity.end() || !overCapacityDiagnostics.CapacityExceeded ||
        overIdentityCapacity.size() != originalSize)
        throw std::runtime_error("selector failed closed or mutated its queue at identity capacity");

    std::list<Event> overEventCapacity;
    for (std::size_t index = 0; index <= Scheduler::kMaxPendingEvents; ++index)
        overEventCapacity.push_back({static_cast<uint64_t>(index + 1), 512, true});
    Scheduler::SelectionDiagnostics eventCapacityDiagnostics;
    if (Scheduler::SelectNext(overEventCapacity, 0, 0, 0us, 0, &eventCapacityDiagnostics) != overEventCapacity.end() ||
        !eventCapacityDiagnostics.CapacityExceeded)
        throw std::runtime_error("selector did not reject a queue beyond its event bound");

    std::cout << "SELECTOR_MAX_LOAD events=" << Scheduler::kMaxPendingEvents
              << " identities=" << Scheduler::kMaxDistinctPendingIdentities
              << " first_update_selections=" << selectedCount
              << " scanned=" << scannedEvents << " probes=" << probeSteps
              << " max_probes_per_select=" << maxProbeSteps
              << " wall_us=" << wallElapsed
              << " lock_metric=test_mutex_proxy_not_RED4ext_lock\n";
    return 0;
}

void TestSelectorBoundednessWithWatchdog(const std::filesystem::path& executable) {
#ifdef _WIN32
    SECURITY_ATTRIBUTES security{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
    HANDLE readPipe = nullptr;
    HANDLE writePipe = nullptr;
    if (!CreatePipe(&readPipe, &writePipe, &security, 0))
        throw std::runtime_error("could not create selector watchdog output pipe");
    SetHandleInformation(readPipe, HANDLE_FLAG_INHERIT, 0);
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = writePipe;
    startup.hStdError = writePipe;
    PROCESS_INFORMATION process{};
    const auto fullPath = std::filesystem::absolute(executable).wstring();
    std::wstring command = L"\"" + fullPath + L"\" --selector-boundedness-child";
    std::vector<wchar_t> writableCommand(command.begin(), command.end());
    writableCommand.push_back(L'\0');
    if (!CreateProcessW(fullPath.c_str(), writableCommand.data(), nullptr, nullptr, TRUE,
            CREATE_NO_WINDOW, nullptr, nullptr, &startup, &process)) {
        CloseHandle(readPipe);
        CloseHandle(writePipe);
        throw std::runtime_error("could not launch selector watchdog child");
    }
    CloseHandle(writePipe);
    const auto wait = WaitForSingleObject(process.hProcess, 5000);
    if (wait == WAIT_TIMEOUT) {
        TerminateProcess(process.hProcess, 124);
        WaitForSingleObject(process.hProcess, INFINITE);
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
        CloseHandle(readPipe);
        throw std::runtime_error("selector boundedness child exceeded 5-second watchdog");
    }
    DWORD exitCode = 1;
    GetExitCodeProcess(process.hProcess, &exitCode);
    CloseHandle(process.hThread);
    CloseHandle(process.hProcess);
    std::string output;
    std::array<char, 512> buffer{};
    DWORD bytesRead = 0;
    while (ReadFile(readPipe, buffer.data(), static_cast<DWORD>(buffer.size()), &bytesRead, nullptr) && bytesRead > 0)
        output.append(buffer.data(), bytesRead);
    CloseHandle(readPipe);
    std::cout << output;
    if (wait != WAIT_OBJECT_0 || exitCode != 0)
        throw std::runtime_error("selector boundedness child failed: " + output);
#else
    const auto fullPath = std::filesystem::absolute(executable);
    const pid_t child = fork();
    if (child < 0) throw std::runtime_error("could not fork selector watchdog child");
    if (child == 0) {
        execl(fullPath.c_str(), fullPath.filename().c_str(), "--selector-boundedness-child", static_cast<char*>(nullptr));
        _exit(127);
    }
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto result = waitpid(child, &status, WNOHANG);
        if (result == child) {
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
                throw std::runtime_error("selector boundedness child failed");
            return;
        }
        if (result < 0) throw std::runtime_error("could not inspect selector watchdog child");
        std::this_thread::sleep_for(10ms);
    }
    kill(child, SIGKILL);
    waitpid(child, &status, 0);
    throw std::runtime_error("selector boundedness child exceeded 5-second watchdog");
#endif
}
void TestProductionSessionGate() {
    SocketSessionGate gate;
    if (gate.Accepting() || !gate.StartSession() || !gate.Accepting())
        throw std::runtime_error("first Running entry did not enable service");
    const auto firstEpoch = gate.Epoch();
    gate.StopSession();
    if (gate.IsCurrent(firstEpoch)) throw std::runtime_error("session exit left old callback epoch current");
    if (gate.Accepting() || !gate.StartSession() || !gate.Accepting())
        throw std::runtime_error("Running re-entry did not re-arm service");
    if (gate.Epoch() == firstEpoch || gate.IsCurrent(firstEpoch))
        throw std::runtime_error("Running re-entry did not invalidate old callbacks");
    gate.Unload();
    if (gate.Accepting() || gate.StartSession())
        throw std::runtime_error("terminal unload was incorrectly re-armed");
}

void TestProductionCallbackDeliveryGate() {
    // A update pump is allowed to delay or reorder these invocations. N's
    // terminal event must not mutate the currently connected N+1 consumer.
    if (ShouldDeliverSocketCallback(SocketCallbackKind::Disconnection, 12, 14,
            SocketLifecycleState::Connected, false, true))
        throw std::runtime_error("stale N disconnection could affect N+1");
    if (ShouldDeliverSocketCallback(SocketCallbackKind::Connection, 12, 14,
            SocketLifecycleState::Connected, false, true))
        throw std::runtime_error("stale N connection could overwrite N+1");
    if (!ShouldDeliverSocketCallback(SocketCallbackKind::Connection, 14, 14,
            SocketLifecycleState::Connected, false, true))
        throw std::runtime_error("current N+1 connection notification was suppressed");
    if (!ShouldDeliverSocketCallback(SocketCallbackKind::Disconnection, 15, 15,
            SocketLifecycleState::Disconnected, false, true))
        throw std::runtime_error("current disconnected notification was suppressed");
    if (ShouldDeliverSocketCallback(SocketCallbackKind::Disconnection, 15, 15,
            SocketLifecycleState::Connected, false, true))
        throw std::runtime_error("current connection was hidden by delayed disconnection");
    if (ShouldDeliverSocketCallback(SocketCallbackKind::Connection, 14, 14,
            SocketLifecycleState::Connected, true, true) ||
        ShouldDeliverSocketCallback(SocketCallbackKind::Connection, 14, 14,
            SocketLifecycleState::Connected, false, false))
        throw std::runtime_error("destroyed socket or old session delivered callback");
}

void TestProductionOwnerDrainAndStopRaces() {
    SocketOwner owner;
    asio::steady_timer timer(owner.GetExecutor());
    std::atomic_int completions{0};
    std::atomic_int finalizers{0};
    std::promise<void> armed;
    if (!owner.Post([&] {
            timer.expires_after(30s);
            timer.async_wait([&](const asio::error_code& error) {
                if (error == asio::error::operation_aborted) ++completions;
            });
            armed.set_value();
        }, 0, true)) throw std::runtime_error("could not arm production owner timer");
    if (armed.get_future().wait_for(5s) != std::future_status::ready)
        throw std::runtime_error("owner timer did not arm");

    std::array<bool, 8> results{};
    std::vector<std::thread> stoppers;
    for (auto& result : results) {
        stoppers.emplace_back([&owner, &finalizers, &result, &timer, &completions] {
            result = owner.Stop([&] {
                ++finalizers;
                asio::error_code ignored;
                timer.cancel(ignored);
            });
        });
    }
    for (auto& stopper : stoppers) stopper.join();
    if (finalizers.load() != 1) throw std::runtime_error("concurrent Stop ran finalizer more than once");
    for (const auto result : results)
        if (!result) throw std::runtime_error("concurrent cooperative Stop did not complete successfully");
    if (completions.load() != 1) throw std::runtime_error("cancel completion was discarded instead of drained");
    // Repeat after the first stop to exercise idempotent join/reap.
    if (!owner.Stop([] {})) throw std::runtime_error("repeated Stop did not reap cleanly");
}

void TestProductionOwnerSlowHandlerCompletesCooperatively() {
    SocketOwner owner;
    std::promise<void> started;
    auto startedFuture = started.get_future();
    if (!owner.Post([&] {
            started.set_value();
            std::this_thread::sleep_for(120ms);
        })) throw std::runtime_error("could not queue recoverable slow handler");
    if (startedFuture.wait_for(5s) != std::future_status::ready)
        throw std::runtime_error("slow handler did not start");
    const auto begin = std::chrono::steady_clock::now();
    if (!owner.Stop([] {})) throw std::runtime_error("cooperative slow handler did not stop cleanly");
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    if (elapsed < 80ms || elapsed > 5s)
        throw std::runtime_error("Stop did not wait for the recoverable handler within the diagnostic window");
}

void TestProductionOwnerBlockedHandlerTimeoutIsDiagnosticOnly() {
    SocketOwner owner;
    std::promise<void> started;
    std::promise<void> release;
    auto releaseFuture = release.get_future().share();
    std::atomic_bool finalizerRan{false};
    if (!owner.Post([&] {
            started.set_value();
            releaseFuture.wait();
        })) throw std::runtime_error("could not queue blocked owner handler");
    if (started.get_future().wait_for(5s) != std::future_status::ready)
        throw std::runtime_error("blocked owner handler did not start");
    if (owner.Stop([&] { finalizerRan.store(true, std::memory_order_release); }, 25ms))
        throw std::runtime_error("blocked handler was reported stopped before its queued finalizer ran");
    if (finalizerRan.load(std::memory_order_acquire))
        throw std::runtime_error("finalizer ran concurrently with the blocked handler");
    release.set_value();
    if (!owner.Stop([] {}, 5s))
        throw std::runtime_error("external reap failed after the cooperative handler was released");
    if (!finalizerRan.load(std::memory_order_acquire))
        throw std::runtime_error("queued owner finalizer was lost after the timeout");
}

void TestProductionOwnerRecoversFromHandlerException() {
    SocketOwner owner;
    std::promise<void> afterException;
    auto reached = afterException.get_future();
    if (!owner.Post([] { throw std::runtime_error("intentional harness exception"); }) ||
        !owner.Post([&] { afterException.set_value(); }))
        throw std::runtime_error("could not queue exception recovery regression");
    if (reached.wait_for(5s) != std::future_status::ready)
        throw std::runtime_error("owner failed to resume after handler exception");
    int finalizerCount = 0;
    if (owner.Stop([&] { ++finalizerCount; }))
        throw std::runtime_error("Stop hid the observed handler exception");
    if (finalizerCount != 1) throw std::runtime_error("finalizer did not run after handler exception");
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--selector-boundedness-child") {
        try { return RunSelectorBoundednessCases(); }
        catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
    }
    try { TestSelectorBoundednessWithWatchdog(std::filesystem::absolute(argv[0])); }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 2; }
    try {
        // First run the physical-incident mixed-port scenario. The helper
        // closes its isolated HUD listener before the regular dual-port lab.
        TestRefusedTelemetryWhileHudAlive();
        LoopbackServer server;
        TestProductionClosePlan();
        TestProductionEventBudget();
        TestProductionEventDispatchBudgetAndFairness();
        TestProductionSessionGate();
        TestProductionCallbackDeliveryGate();
        TestProductionOwnerDrainAndStopRaces();
        TestProductionOwnerSlowHandlerCompletesCooperatively();
        TestProductionOwnerBlockedHandlerTimeoutIsDiagnosticOnly();
        TestProductionOwnerRecoversFromHandlerException();
        ClientChannel telemetry;
        ClientChannel hud;
        std::string largeJson = "{\"channel\":\"telemetry\",\"data\":\"" + std::string(65536, 'x') + "\"}";
        auto telemetryAck = telemetry.SendAndRead(server.TelemetryPort(), largeJson);
        auto hudAck = hud.SendAndRead(server.HudPort(), "SHOW|h1");
        if (telemetryAck.wait_for(5s) != std::future_status::ready || hudAck.wait_for(5s) != std::future_status::ready) return 10;
        if (telemetryAck.get() != "ACK|TELEMETRY" || hudAck.get() != "RECEIVED|SHOW|h1") return 11;
        auto reset = telemetry.WaitForDisconnect();
        auto hudSecond = hud.SendAgain("HIDE|h1");
        if (reset.wait_for(5s) != std::future_status::ready || hudSecond.wait_for(5s) != std::future_status::ready) return 12;
        if (!reset.get() || hudSecond.get() != "RECEIVED|HIDE|h1") return 13;
        if (!telemetry.AffinityOk() || !hud.AffinityOk()) return 14;
        telemetry.Stop();
        auto canceledRead = hud.ArmPendingRead();
        hud.Stop();
        if (canceledRead.wait_for(5s) != std::future_status::ready || !canceledRead.get()) return 32;

        ClientChannel selfStopOwner;
        auto selfStop = selfStopOwner.StopFromOwner();
        if (selfStop.wait_for(5s) != std::future_status::ready || selfStop.get()) return 15;
        selfStopOwner.Stop();

        SocketOwner pressuredOwner;
        SocketOwner isolatedOwner;
        std::promise<void> workerBlocked;
        std::promise<void> releaseWorker;
        const auto releaseSignal = releaseWorker.get_future().share();
        if (!pressuredOwner.Post([&workerBlocked, releaseSignal] {
                workerBlocked.set_value();
                releaseSignal.wait();
            })) return 17;
        if (workerBlocked.get_future().wait_for(5s) != std::future_status::ready) return 18;
        for (std::size_t index = 0; index < SocketOwner::kMaxQueuedTasks - 1; ++index) {
            if (!pressuredOwner.Post([] {})) return 19;
        }
        if (pressuredOwner.Post([] {})) return 24;
        if (!isolatedOwner.Post([] {})) return 25;
        releaseWorker.set_value();
        if (!pressuredOwner.Stop([] {}) || !isolatedOwner.Stop([] {})) return 26;

        SocketOwner byteOwner;
        std::promise<void> byteWorkerBlocked;
        std::promise<void> releaseByteWorker;
        const auto releaseByteSignal = releaseByteWorker.get_future().share();
        if (!byteOwner.Post([&byteWorkerBlocked, releaseByteSignal] {
                byteWorkerBlocked.set_value();
                releaseByteSignal.wait();
            })) return 27;
        if (byteWorkerBlocked.get_future().wait_for(5s) != std::future_status::ready) return 28;
        if (!byteOwner.Post([] {}, SocketOwner::kMaxQueuedBytes)) return 29;
        if (byteOwner.Post([] {}, 1)) return 30;
        releaseByteWorker.set_value();
        if (!byteOwner.Stop([] {})) return 31;

        LifecycleProbe lifecycle;
        for (int round = 0; round < 100; ++round) {
            const auto generation = lifecycle.Connect();
            std::vector<std::thread> closers;
            for (int index = 0; index < 32; ++index) {
                closers.emplace_back([&lifecycle, generation] { lifecycle.CloseIfGeneration(generation); });
            }
            for (auto& closer : closers) closer.join();
            auto snapshot = lifecycle.Snapshot();
            snapshot = lifecycle.Snapshot(); // drains nested reentrant close requests
            if (snapshot[0] != 0 || snapshot[1] != round + 1 || snapshot[2] != round + 1) return 20;
        }
        const auto oldGeneration = lifecycle.Connect();
        lifecycle.CloseIfGeneration(oldGeneration);
        (void)lifecycle.Snapshot();
        (void)lifecycle.Snapshot();
        const auto newGeneration = lifecycle.Connect();
        if (newGeneration == oldGeneration) return 21;
        lifecycle.CloseIfGeneration(oldGeneration);
        auto staleSnapshot = lifecycle.Snapshot();
        if (staleSnapshot[0] != 1 || staleSnapshot[1] != 101 || staleSnapshot[2] != 101) return 22;
        lifecycle.CloseIfGeneration(newGeneration);
        staleSnapshot = lifecycle.Snapshot();
        if (staleSnapshot[0] != 0 || staleSnapshot[1] != 102 || staleSnapshot[2] != 102) return 23;
        lifecycle.Stop();

        std::cout << "TELEMETRY REFUSAL + HUD PASS: 10 refused attempts to 20770 while SHOW/HIDE on 20771, owner teardown verified\n";
        std::cout << "ASIO CONCURRENT LOOPBACK PASS: 20770 large JSON/CRLF + reset; 20771 SHOW/HIDE remains independent\n";
        std::cout << "PRODUCTION CLOSE PLAN PASS: duplicate close idempotent; stale N cannot close N+1; disconnected destroy and Closing verified\n";
        std::cout << "PRODUCTION EVENT BUDGET PASS: per-channel isolation, reserved critical lane, finite saturation and pump recovery\n";
        std::cout << "PRODUCTION SESSION GATE PASS: inactive before Running, repeated session re-entry, unload terminal\n";
        std::cout << "PRODUCTION CALLBACK GATE PASS: delayed/reordered N events cannot affect active N+1; current events preserved\n";
        std::cout << "OWNER DRAIN/STOP RACES PASS: cancel completion drained once; finalizer and join idempotent under concurrent Stop\n";
        std::cout << "OWNER SLOW HANDLER PASS: cooperative 120ms handler completed before shutdown and join\n";
        std::cout << "OWNER BLOCKED HANDLER PASS: diagnostic timeout returned without stopping or detaching; external reap succeeded after release\n";
        std::cout << "PRODUCTION UPDATE SCHEDULER PASS: per-socket causality, bounded critical latency, critical-storm fairness, count/byte/soft-time budgets, 20770/20771 progress\n";
        std::cout << "OWNER EXCEPTION RECOVERY PASS: reactor resumes, finalizer runs once, failure is reported\n";
        std::cout << "OWNER SELF-STOP PASS: worker schedules finalizer; external Stop reaps joinable worker without detach\n";
        std::cout << "OWNER BACKPRESSURE PASS: queued task/byte caps reject excess; independent owner continues\n";
        std::cout << "LIFECYCLE MODEL PASS (supplemental): 100x32 concurrent close requests and stale generation N cannot close N+1\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ASIO CONCURRENT LOOPBACK FAIL: " << error.what() << "\n";
        return 2;
    }
}
