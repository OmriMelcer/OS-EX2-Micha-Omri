// test28_isdone_after_wait: for N in {1,2,16}, after wait() assert isDone()==true.
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

class IdentityClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        context.addIntermediate(std::dynamic_pointer_cast<IntEl>(key),
                                std::dynamic_pointer_cast<IntEl>(value));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        for (auto &p : pairs)
            context.addOutput(std::dynamic_pointer_cast<K3>(p.first),
                              std::dynamic_pointer_cast<V3>(p.second));
    }
};

bool test_for_n(int N) {
    IdentityClient client;
    InputVec inputVec;
    for (int i = 0; i < 20; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(i * 5)});
    }
    MapReduceJob job(client, inputVec, N);
    job.wait();

    // Bounded check
    bool done = false;
    for (int i = 0; i < 2000 && !done; i++) {
        done = job.isDone();
        if (!done) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!done) {
        std::cerr << "FAIL: N=" << N << ": isDone() returned false after wait()\n";
        return false;
    }
    return true;
}

int main() {
    int thread_levels[] = {1, 2, 16};
    for (int N : thread_levels) {
        if (!test_for_n(N)) return 1;
    }
    std::cerr << "PASS: isDone() true after wait() for N=1,2,16\n";
    return 0;
}
