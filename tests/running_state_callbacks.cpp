#include "../src/RunningStateCallback.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {
void Check(bool p_condition, const char* p_expression, int p_line) {
    if (p_condition) return;
    std::cerr << "CHECK failed at line " << p_line << ": " << p_expression << '\n';
    std::exit(2);
}
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

enum class StateEvent { Enter, Update, Exit };
struct ProductionAdapter {
    bool terminal{false};
    unsigned enter{0}, update{0}, exit{0}, delivered{0};
    bool OnEnter() { return RunPersistentStateCallback(terminal, [&] { ++enter; }); }
    bool OnUpdate() { return RunPersistentStateCallback(terminal, [&] { ++update; ++delivered; }); }
    bool OnExit() { return RunPersistentStateCallback(terminal, [&] { ++exit; }); }
};

// Mirrors RED4ext StateSystem::Run: true means erase this callback.
template<class Callback> void Dispatch(bool& p_registered, Callback&& p_callback) {
    if (p_registered && p_callback()) p_registered = false;
}
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--probe-check-failure") CHECK(false);
    ProductionAdapter adapter;
    bool enterRegistered = true, updateRegistered = true, exitRegistered = true;
    auto enter = [&] { return adapter.OnEnter(); };
    auto update = [&] { return adapter.OnUpdate(); };
    auto exit = [&] { return adapter.OnExit(); };

    // At least three ticks per activation, two complete Running cycles.
    for (int cycle = 0; cycle != 2; ++cycle) {
        Dispatch(enterRegistered, enter);
        for (int tick = 0; tick != 3; ++tick) Dispatch(updateRegistered, update);
        Dispatch(exitRegistered, exit);
    }
    CHECK(enterRegistered && updateRegistered && exitRegistered);
    CHECK(adapter.enter == 2 && adapter.exit == 2 && adapter.update == 6);
    CHECK(adapter.delivered == 6);

    // Sustained pump proves callback is not erased after its first invocation.
    for (int tick = 0; tick != 1000; ++tick) Dispatch(updateRegistered, update);
    CHECK(adapter.update == 1006 && adapter.delivered == 1006 && updateRegistered);

    // Terminal unload disables work and allows the host to erase the callback.
    adapter.terminal = true;
    const auto before = adapter.delivered;
    Dispatch(updateRegistered, update);
    CHECK(!updateRegistered && adapter.delivered == before);

    // Negative control: the host-faithful dispatcher erases a callback that
    // returns true after the first invocation, reproducing the R7 regression.
    bool r7Registered = true;
    unsigned r7Calls = 0;
    const auto r7Callback = [&] { ++r7Calls; return true; };
    Dispatch(r7Registered, r7Callback);
    Dispatch(r7Registered, r7Callback);
    CHECK(!r7Registered && r7Calls == 1);
    std::cout << "RUNNING_CALLBACKS_PASS ticks=1006 cycles=2 events=1006 terminal=closed\n";
}
