// Compile actual RedSocket.cpp and SocketService.cpp against narrow test-only
// RED API doubles. Real Asio loopback, no game, DLL load or native ABI claim.
#include "RedSocket.h"
#include "SocketService.h"
#include "Logger.h"

#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace std::chrono_literals;
using asio::ip::tcp;
#define CHECK(x) do {if(!(x)) throw std::runtime_error(#x);}while(false)

static void pump() {
    auto& service=SocketService::Get();
    auto lease=service.BeginUpdate();
    CHECK(lease);
    service.Update(lease);
}
template<class Predicate> static void until(Predicate predicate) {
    const auto deadline=std::chrono::steady_clock::now()+5s;
    while(!predicate()) {
        if(std::chrono::steady_clock::now()>=deadline) throw std::runtime_error("bounded wait expired");
        pump();std::this_thread::sleep_for(1ms);
    }
}
struct Listener : Red::IScriptable {
    int connected=0, disconnected=0, errors=0, failedConnections=0;
    std::vector<std::string> commands;
    void Invoke(const std::string& name,const Red::CallbackArgument& arg) override {
        if(name=="connect") {if(std::get<int32_t>(arg)==0) ++connected; else ++failedConnections;}
        else if(name=="disconnect")++disconnected;
        else if(name=="error")++errors;
        else if(name=="line")commands.push_back(std::get<std::string>(arg));
    }
};
static void registerListener(const Red::Handle<RedSocket>& s,const Red::Handle<Listener>& l) {
    CHECK(s->TryRegisterListener(l,"line","connect",Red::Optional<Red::CName>("disconnect")));
}
static void connect(const Red::Handle<RedSocket>& s,const Red::Handle<Listener>& l,tcp::acceptor& acceptor,tcp::socket& peer, bool dispatch=true) {
    registerListener(s,l);
    s->Connect("127.0.0.1",acceptor.local_endpoint().port());
    // A queued or active connection cannot replace its listener.
    CHECK(!s->TryRegisterListener(l,"line","connect",Red::Optional<Red::CName>("disconnect")));
    acceptor.non_blocking(true);
    const auto deadline=std::chrono::steady_clock::now()+5s;
    for(;;) {
        asio::error_code error;
        acceptor.accept(peer,error);
        if(!error)break;
        CHECK(error==asio::error::would_block || error==asio::error::try_again);
        CHECK(std::chrono::steady_clock::now()<deadline);
        std::this_thread::sleep_for(1ms);
    }
    if(dispatch) until([&]{return l->connected==1 && s->IsConnected();});
    else {
        while(!s->IsConnected()) {
            CHECK(std::chrono::steady_clock::now()<deadline);
            std::this_thread::sleep_for(1ms);
        }
    }
}

struct SocketTestAccess {
    static void FailResolve(const Red::Handle<RedSocket>& s) { s->m_failResolveForTest.store(true); }
    static void FailWrite(const Red::Handle<RedSocket>& s) { s->m_failWriteForTest.store(true); }
    static void FailPost(const Red::Handle<RedSocket>& s) { s->m_owner.FailNextPostForTest(); }
    static void FailFinalizer(const Red::Handle<RedSocket>& s) { s->m_owner.FailNextFinalizerForTest(); }
    using Epoch=std::pair<uint64_t,std::shared_ptr<SocketTransportEpoch>>;
    static Epoch Snapshot(const Red::Handle<RedSocket>& s) {
        auto result=std::make_shared<std::promise<Epoch>>();
        auto done=result->get_future();
        CHECK(s->m_owner.Post([raw=s.GetPtr(),result] {
            result->set_value({raw->m_generation.load(),raw->m_transport});
        }));
        CHECK(done.wait_for(5s)==std::future_status::ready);
        return done.get();
    }
    static void LateCompletion(const Red::Handle<RedSocket>& s,const Epoch& old) {
        auto finished=std::make_shared<std::promise<void>>();
        auto done=finished->get_future();
        CHECK(s->m_owner.Post([raw=s.GetPtr(),old,finished] {
            try {
                CHECK(old.second && raw->m_transport!=old.second && !old.second->Socket.is_open());
                raw->HandleRead(old.first,old.second,asio::error::connection_reset,0);
                raw->HandleWrite(old.first,asio::error::connection_reset,0);
                CHECK(raw->IsConnected());
                finished->set_value();
            } catch(...) {finished->set_exception(std::current_exception());}
        }));
        CHECK(done.wait_for(5s)==std::future_status::ready);done.get();
    }

    static void InjectWriteFailure(const Red::Handle<RedSocket>& s,uint64_t id) {
        auto finished=std::make_shared<std::promise<void>>();
        auto done=finished->get_future();
        CHECK(s->m_owner.Post([raw=s.GetPtr(),id,finished] {
            try {
                auto payload=SocketWritePayload::Create("test\r\n");
                CHECK(payload);
                raw->m_pendingWriteBytes=payload->Text.size();
                raw->m_writeQueue.push_back({std::move(payload),id});
                raw->m_writeInProgress=true;
                raw->HandleWrite(raw->m_generation.load(),asio::error::connection_reset,0);
                finished->set_value();
            } catch(...) {finished->set_exception(std::current_exception());}
        }));
        CHECK(done.wait_for(5s)==std::future_status::ready);done.get();
    }
};

static void TestFailurePaths() {
    auto& service=SocketService::Get();
    service.StartSession();
    std::vector<Red::Handle<RedSocket>> sockets;
    sockets.reserve(SocketEventBudget::kMaxSockets);
    for(std::size_t i=0;i<SocketEventBudget::kMaxSockets;++i) {
        auto s=RedSocket::Create();
        CHECK(s && !s->IsRetired());
        sockets.push_back(s);
    }
    CHECK(!RedSocket::Create());
    RedSocket::Destroy(sockets.front());
    CHECK(sockets.front()->IsRetired());
    auto slot=RedSocket::Create();
    CHECK(slot);
    RedSocket::Destroy(slot);
    for(auto& s:sockets) RedSocket::Destroy(s);
    sockets.clear();
    std::cout<<"ACTUAL_CAP_RETIRE_READMISSION_PASS\n";

    asio::io_context io;
    tcp::acceptor acceptor(io,{asio::ip::address_v4::loopback(),0});
    auto a=RedSocket::Create(), b=RedSocket::Create();
    CHECK(a && b);
    auto rejected=Red::MakeHandle<Listener>();
    registerListener(a,rejected);
    SocketTestAccess::FailResolve(a);
    a->Connect("127.0.0.1",acceptor.local_endpoint().port());
    until([&]{return rejected->failedConnections==1;});
    CHECK(!a->IsConnected() && !a->IsRetired());

    auto la=Red::MakeHandle<Listener>(), lb=Red::MakeHandle<Listener>();
    tcp::socket pa(io),pb(io);
    connect(a,la,acceptor,pa);
    connect(b,lb,acceptor,pb);
    SocketTestAccess::FailWrite(a);
    a->SendCommand("fault",{},Red::Optional<Red::Handle<Red::IScriptable>>(la),Red::Optional<Red::CName>("error"));
    until([&]{return la->errors==1 && la->disconnected==1;});
    CHECK(!a->IsConnected() && b->IsConnected());
    std::cout<<"ACTUAL_RESOLVE_WRITE_INITIATION_FAULT_PASS\n";

    auto next=Red::MakeHandle<Listener>();tcp::socket pn(io);
    connect(a,next,acceptor,pn);
    // Snapshot runs after read initiation on the owner. Keep only a weak
    // witness: the outstanding read must release its transport even if a
    // native caller keeps the retired socket alive.
    auto epoch=SocketTestAccess::Snapshot(a);
    CHECK(epoch.second && epoch.second->Socket.is_open());
    std::weak_ptr<SocketTransportEpoch> weak=epoch.second;
    epoch.second.reset();
    SocketTestAccess::FailFinalizer(a);
    RedSocket::Destroy(a);
    CHECK(a->IsRetired() && !a->IsConnected() && !a->HasListener());
    CHECK(weak.expired());
    CHECK(b->IsConnected());
    std::cout<<"ACTUAL_FINALIZER_SUBMISSION_FAULT_DRAIN_PASS\n";

    SocketTestAccess::FailPost(b);
    b->Disconnect();
    CHECK(b->IsRetired() && !b->IsConnected() && !b->HasListener());
    auto c=RedSocket::Create();
    CHECK(c && !c->IsRetired());
    RedSocket::Destroy(c);
    RedSocket::Destroy(b);
    pump();
    service.Stop();
    CHECK(SocketResourceBudget::Used()==0);
    std::cout<<"ACTUAL_CRITICAL_POST_REJECT_RETIRE_PASS\n";
}

static void TestProductionService() {
    auto& service=SocketService::Get();
    CHECK(!RedSocket::Create()); // inactive admission is observably refused
    service.StartSession();
    auto a=RedSocket::Create(), b=RedSocket::Create();
    CHECK(a && b);
    auto la=Red::MakeHandle<Listener>(), lb=Red::MakeHandle<Listener>();
    CHECK(!a->TryRegisterListener({},"line","connect",{}));
    asio::io_context io;
    tcp::acceptor acceptor(io,{asio::ip::address_v4::loopback(),0});
    tcp::socket pa(io),pb(io);
    connect(a,la,acceptor,pa);connect(b,lb,acceptor,pb);
    // Real parser, dispatch and bridge conversion.
    asio::write(pb,asio::buffer("alpha\nbeta\r\n",12));
    until([&]{return lb->commands.size()==2;});
    CHECK(lb->commands[0]=="alpha" && lb->commands[1]=="beta");

    // Actual failed-write disposition with a test-controlled completion,
    // no game or real network fault timing assumption.
    const auto id=service.RegisterSendError(la,"error",a.GetPtr(),a->GetEventIdentity());
    CHECK(id!=0);SocketTestAccess::InjectWriteFailure(a,id);
    CHECK(!a->IsConnected());
    until([&]{return la->errors==1 && la->disconnected==1;});
    pump();CHECK(la->errors==1);

    // Per-identity critical saturation, followed by actual retirement/purge.
    int old=0, healthy=0;
    auto countOld=[&](const Red::Handle<RedSocket>&){++old;};
    auto countHealthy=[&](const Red::Handle<RedSocket>&){++healthy;};
    for(std::size_t i=0;i<SocketEventBudget::kMaxCriticalPerSocket;++i)
        CHECK(service.QueueEvent(a.GetPtr(),a->GetEventIdentity(),countOld,0,true));
    CHECK(!service.QueueEvent(a.GetPtr(),a->GetEventIdentity(),countOld,0,true));
    CHECK(service.QueueEvent(b.GetPtr(),b->GetEventIdentity(),countHealthy,0,true));
    const auto staleId=service.RegisterSendError(la,"error",a.GetPtr(),a->GetEventIdentity());
    CHECK(staleId);service.QueueSendError(staleId,a.GetPtr(),a->GetEventIdentity());
    RedSocket::Destroy(a);
    CHECK(a->IsRetired() && !a->HasListener());
    CHECK(!service.QueueEvent(a.GetPtr(),a->GetEventIdentity(),countOld,0,true));
    pump();CHECK(old==0 && healthy==1 && la->errors==1 && b->IsConnected());

    // Input length is not a C-string prefix. Invalid input rejected with one
    // eligible callback, no payload reaches peer.
    b->SendCommand(Red::CString("a\0b",3),{},Red::Optional<Red::Handle<Red::IScriptable>>(lb),Red::Optional<Red::CName>("error"));
    until([&]{return lb->errors==1;});pump();CHECK(lb->errors==1);
    asio::write(pb,asio::buffer("ok\n",3));
    until([&]{return lb->commands.size()==3;});
    CHECK(lb->commands.back()=="ok");

    // Complete lines accepted before orderly EOF must drain even if the
    // game pump has not yet delivered connection success. Partial tail drops.
    auto e=RedSocket::Create(); auto le=Red::MakeHandle<Listener>();
    tcp::socket pe(io);
    connect(e,le,acceptor,pe,false);
    auto epoch=SocketTestAccess::Snapshot(e);
    CHECK(le->connected==0);
    asio::write(pe,asio::buffer("finished\npartial",16));
    pe.shutdown(tcp::socket::shutdown_send);
    const auto deadline=std::chrono::steady_clock::now()+5s;
    while(e->IsConnected()) {
        CHECK(std::chrono::steady_clock::now()<deadline);
        std::this_thread::sleep_for(1ms);
    }
    until([&]{return le->disconnected==1;});
    CHECK(le->connected==1 && le->commands==std::vector<std::string>{"finished"});
    auto next=Red::MakeHandle<Listener>();tcp::socket pn(io);
    connect(e,next,acceptor,pn);
    SocketTestAccess::LateCompletion(e,epoch);
    asio::write(pn,asio::buffer("new\n",4));
    until([&]{return next->commands.size()==1;});
    CHECK(next->commands[0]=="new" && next->disconnected==0);
    RedSocket::Destroy(e);
    epoch.second.reset();

    // Reentrant unload is refused, cleanup remains under the active lease.
    bool refused=false;
    CHECK(service.QueueEvent(b.GetPtr(),b->GetEventIdentity(),[&](const Red::Handle<RedSocket>&) {
        CHECK(!service.BeginUpdate());
        refused=!service.Unload();
    }));
    pump();
    CHECK(refused && service.IsUnloaded() && b->IsRetired() && !b->HasListener());
    CHECK(!RedSocket::Create());
    CHECK(service.Unload());
    for(const auto& log:RED4ext::v1::Logger::lines)
        CHECK(log.find("127.0.0.1")==std::string::npos && log.find("alpha")==std::string::npos);
}

static void TestBudgetAllocationRollback() {
    SocketEventBudget budget;
    CHECK(!budget.TryEnqueue(1,512,true,[]{throw std::bad_alloc();}));
    CHECK(budget.CriticalCount()==0);
    CHECK(!budget.TryEnqueue(1,512,false,[]{throw std::bad_alloc();}));
    CHECK(budget.EventCount()==0 && budget.ByteCount()==0);
    CHECK(budget.TryEnqueue(2,512,true,[]{}));
}

static void TestAbortedOwnerDiscardsCommands() {
    SocketOwner owner;
    const auto baseline=SocketResourceBudget::Used();
    std::promise<void> entered, release;
    auto gate=release.get_future().share();
    std::atomic_bool ran{false};
    CHECK(owner.Post([&]{entered.set_value();gate.wait();}));
    entered.get_future().wait();
    CHECK(owner.Post([&]{ran.store(true);},65536));
    owner.FailNextFinalizerForTest();
    auto stopped=std::async(std::launch::async,[&]{return owner.Stop([]{});});
    const auto deadline=std::chrono::steady_clock::now()+5s;
    while(!owner.AbortingForTest() && std::chrono::steady_clock::now()<deadline)
        std::this_thread::sleep_for(1ms);
    const bool aborted=owner.AbortingForTest();
    release.set_value();
    CHECK(!stopped.get() && aborted);
    CHECK(owner.DrainAbortedAfterJoin() && owner.DrainAbortedAfterJoin());
    CHECK(!ran.load() && SocketResourceBudget::Used()==baseline);
    std::cout<<"ABORTED_OWNER_DISCARDS_QUEUED_COMMANDS_PASS\n";
}

static void TestExternalUnloadWait() {
    auto& service=SocketService::Get();
    service.StartSession();
    auto s=RedSocket::Create();
    auto l=Red::MakeHandle<Listener>();
    CHECK(s);registerListener(s,l);
    std::promise<void> callbackEntered, callbackRelease, tailEntered, tailRelease, unloadEntered;
    auto callbackGate=callbackRelease.get_future().share();
    auto tailGate=tailRelease.get_future().share();
    CHECK(service.QueueEvent(s.GetPtr(),s->GetEventIdentity(),[&](const Red::Handle<RedSocket>&) {
        service.Stop(); // reentrant stop; actual service defers cleanup
        callbackEntered.set_value();
        callbackGate.wait();
    }));
    auto update=std::async(std::launch::async,[&] {
        auto lease=service.BeginUpdate();
        if(!lease)throw std::runtime_error("missing update lease");
        service.Update(lease);
        tailEntered.set_value();
        tailGate.wait();
    });
    callbackEntered.get_future().wait();
    auto unload=std::async(std::launch::async,[&]{unloadEntered.set_value();return service.Unload();});
    unloadEntered.get_future().wait();
    const bool waitedCallback=unload.wait_for(30ms)==std::future_status::timeout;
    callbackRelease.set_value();
    tailEntered.get_future().wait();
    const bool cleaned=s->IsRetired() && !s->HasListener();
    const bool waitedTail=unload.wait_for(30ms)==std::future_status::timeout;
    tailRelease.set_value();
    update.get();
    CHECK(unload.get() && waitedCallback && cleaned && waitedTail && service.IsUnloaded());
    std::cout<<"ACTUAL_EXTERNAL_UNLOAD_WAITS_CALLBACK_AND_CLEANUP_TAIL_PASS\n";
}

int main(int argc,char** argv) {
    RED4ext::v1::Logger logger;
    RED4ext::v1::Sdk sdk{&logger};
    Logger::Load(nullptr,&sdk);
    try {
        if(argc==2 && std::string_view(argv[1])=="--external-unload") {
            TestExternalUnloadWait();return 0;
        }
        TestBudgetAllocationRollback();TestAbortedOwnerDiscardsCommands();TestFailurePaths();TestProductionService();
    }
    catch(const std::exception& e) {
        std::cerr<<"FAIL "<<e.what()<<"\n";
        SocketService::Get().Unload();return 2;
    }
    std::cout<<"DEEPBIDE_ACTUAL_PRODUCTION_ROUTES_PASS\n";
    return 0;
}
