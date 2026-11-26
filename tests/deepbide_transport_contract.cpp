// Actual production framing, buffer-retention and worker-reap helpers.
#include "SocketTransportBuffers.h"
#include "SocketOwner.h"

#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace std::chrono_literals;
#define CHECK(expr) do { if (!(expr)) throw std::runtime_error(#expr); } while (false)

static void TestFraming() {
    SocketLineReader input;
    std::string line;
    auto append = [&](std::string_view s) { input.Append(s.data(),s.size()); };
    append("first\r");
    CHECK(input.Take(line,16)==SocketLineResult::NeedMore);
    append("\nsecond\nthird\r\n");
    CHECK(input.Take(line,16)==SocketLineResult::Ready && line=="first");
    CHECK(input.Take(line,16)==SocketLineResult::Ready && line=="second");
    CHECK(input.Take(line,16)==SocketLineResult::Ready && line=="third");
    CHECK(input.Take(line,16)==SocketLineResult::NeedMore);
    append(std::string(16,'x')+"\r");
    CHECK(input.Take(line,16)==SocketLineResult::NeedMore);
    append("\n");
    CHECK(input.Take(line,16)==SocketLineResult::Ready && line.size()==16);
    CHECK(input.Take(line,16)==SocketLineResult::NeedMore);
    append(std::string(17,'x'));
    CHECK(input.Take(line,16)==SocketLineResult::TooLarge);
    CHECK(IsSingleSocketLine(R"({"status":"accepted"})"));
    CHECK(!IsSingleSocketLine("a\nb") && !IsSingleSocketLine("a\rb") &&
          !IsSingleSocketLine("a\r\nb"));
    CHECK(!IsSingleSocketLine(std::string_view("a\0b",3)));
    CHECK(!IsSingleSocketLine("\xc0\x80"));
    CHECK(!IsSingleSocketLine("\xed\xa0\x80"));
    CHECK(IsSingleSocketLine("\xf0\x9f\x98\x80"));
    SocketLineReader bad;
    bad.Append("a\0b\n",4);
    CHECK(bad.Take(line,16)==SocketLineResult::Malformed);
    SocketLineReader large;
    const std::string chunk(4096,'x');
    for(int k=0;k<256;++k) {
        large.Append(chunk.data(),chunk.size());
        CHECK(large.Take(line,1024*1024)==SocketLineResult::NeedMore);
    }
    large.Append("\n",1);
    CHECK(large.Take(line,1024*1024)==SocketLineResult::Ready);
    CHECK(large.ScannedBytes()==1024*1024+1);
}

static void TestAggregateBudget() {
    CHECK(SocketResourceBudget::Used()==0);
    auto a=SocketResourceBudget::Reserve(SocketResourceBudget::kLimit-128);
    CHECK(a && SocketResourceBudget::Used()==SocketResourceBudget::kLimit);
    CHECK(!SocketResourceBudget::Reserve(0));
    a.reset();
    CHECK(SocketResourceBudget::Used()==0);
    auto payload=SocketWritePayload::Create("hello");
    CHECK(payload && SocketResourceBudget::Used()==133);
    auto held=payload;
    payload.reset();
    CHECK(SocketResourceBudget::Used()==133);
    held.reset();
    CHECK(SocketResourceBudget::Used()==0);
}

static void TestPendingWriteOwnsStorage() {
    asio::io_context io;
    using tcp=asio::ip::tcp;
    tcp::acceptor acceptor(io,{asio::ip::address_v4::loopback(),0});
    tcp::socket client(io),peer(io);
    client.connect(acceptor.local_endpoint());acceptor.accept(peer);
    auto payload=std::make_shared<const std::string>(1024*1024,'x');
    std::weak_ptr<const std::string> witness=payload;
    bool completed=false;
    bool canceled=false;
    client.set_option(asio::socket_base::send_buffer_size(1024));
    asio::async_write(client,asio::buffer(*payload),
        RetainSocketBuffer(payload,[&](const auto& retained,const asio::error_code& error,std::size_t){
            canceled=static_cast<bool>(error);
            CHECK(retained->size()==1024*1024 && (*retained)[0]=='x');
            completed=true;
        }));
    // Model queue clear during close, before the asynchronous completion.
    payload.reset();
    CHECK(!witness.expired());
    client.close();io.run();
    CHECK(completed && canceled && witness.expired());
}

static void TestReadEpochHasOwnStorage() {
    asio::io_context io;
    using tcp=asio::ip::tcp;
    tcp::acceptor acceptor(io,{asio::ip::address_v4::loopback(),0});
    tcp::socket client(io),peer(io);
    client.connect(acceptor.local_endpoint());acceptor.accept(peer);
    using Buffer=std::array<char,4096>;
    auto old=std::make_shared<Buffer>();
    std::weak_ptr<Buffer> oldWitness=old;
    bool oldDone=false,newDone=false;
    client.async_read_some(asio::buffer(*old),
        RetainSocketBuffer(old,[&](const auto& retained,const asio::error_code&,std::size_t){
            CHECK(retained==oldWitness.lock());oldDone=true;
        }));
    old.reset();
    client.close();peer.close();
    auto current=std::make_shared<Buffer>();
    CHECK(!oldWitness.expired() && oldWitness.lock().get()!=current.get());
    client.connect(acceptor.local_endpoint());acceptor.accept(peer);
    client.async_read_some(asio::buffer(*current),
        RetainSocketBuffer(current,[&](const auto& retained,const asio::error_code& error,std::size_t n){
            CHECK(!error && n==3 && std::string(retained->data(),n)=="new");newDone=true;
        }));
    asio::write(peer,asio::buffer("new",3));
    io.run();
    CHECK(oldDone && newDone && oldWitness.expired());
}

static void TestExternalReapAfterDiagnosticTimeout() {
    SocketOwner owner;
    std::promise<void> entered,release;
    auto releaseSignal=release.get_future().share();
    CHECK(owner.Post([&]{entered.set_value();releaseSignal.wait();}));
    entered.get_future().wait();
    bool finalized=false;
    const bool clean=owner.Stop([&]{finalized=true;},10ms);
    auto reap=std::async(std::launch::async,[&]{return owner.ReapStoppedWorker();});
    const bool waiting=reap.wait_for(10ms)==std::future_status::timeout;
    release.set_value();
    CHECK(!clean && waiting && reap.get() && finalized);
    CHECK(!owner.Post([]{}));
}

int main() {
    try {
        TestFraming();
        TestAggregateBudget();
        TestPendingWriteOwnsStorage();
        TestReadEpochHasOwnStorage();
        TestExternalReapAfterDiagnosticTimeout();
        std::cout<<"DEEPBIDE_TRANSPORT_CONTRACT_PASS 5/5\n";
    } catch(const std::exception& e) {
        std::cerr<<"FAIL "<<e.what()<<"\n";return 2;
    }
    return 0;
}
