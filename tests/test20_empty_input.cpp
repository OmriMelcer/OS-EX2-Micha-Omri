// test20_empty_input: empty InputVec, N=4.
// Assert wait() returns, isDone()==true (bounded), getOutput() is empty.
#include "MapReduceJob.h"
#include <iostream>
#include <chrono>
#include <thread>

class IntEl : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    int num;
    IntEl(int n) : num(n) {}
    bool operator<(const K1 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K2 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K3 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
};

class NullClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1>, const std::shared_ptr<V1>, MapContext &) const override {}
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        for (auto &p : pairs)
            context.addOutput(std::dynamic_pointer_cast<K3>(p.first),
                              std::dynamic_pointer_cast<V3>(p.second));
    }
};

int main() {
    NullClient client;
    InputVec inputVec; // empty

    MapReduceJob job(client, inputVec, 4);
    job.wait();

    // Bounded isDone check
    bool done = false;
    for (int i = 0; i < 2000 && !done; i++) {
        done = job.isDone();
        if (!done) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!done) {
        std::cerr << "FAIL: isDone() never returned true after wait() with empty input\n";
        return 1;
    }

    auto output = job.getOutput();
    if (!output.empty()) {
        std::cerr << "FAIL: getOutput() not empty for empty input, size=" << output.size() << "\n";
        return 1;
    }

    std::cerr << "PASS: empty input\n";
    return 0;
}
