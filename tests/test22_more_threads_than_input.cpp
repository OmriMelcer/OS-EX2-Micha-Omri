// test22_more_threads_than_input: N=32 but only 3 input pairs; assert no crash/hang and correct output.
#include "MapReduceJob.h"
#include <iostream>

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

int main() {
    IdentityClient client;
    InputVec inputVec;
    // Only 3 input pairs, but 32 threads
    inputVec.push_back({std::make_shared<IntEl>(1), std::make_shared<IntEl>(10)});
    inputVec.push_back({std::make_shared<IntEl>(2), std::make_shared<IntEl>(20)});
    inputVec.push_back({std::make_shared<IntEl>(3), std::make_shared<IntEl>(30)});

    MapReduceJob job(client, inputVec, 32);
    auto output = job.getOutput();

    if (output.size() != 3) {
        std::cerr << "FAIL: expected 3 output pairs, got " << output.size() << "\n";
        return 1;
    }

    // output is sorted by key
    int expected_keys[] = {1, 2, 3};
    int expected_vals[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        auto k = std::static_pointer_cast<IntEl>(output[i].first);
        auto v = std::static_pointer_cast<IntEl>(output[i].second);
        if (k->num != expected_keys[i] || v->num != expected_vals[i]) {
            std::cerr << "FAIL: at index " << i << " expected (" << expected_keys[i]
                      << "," << expected_vals[i] << ") got (" << k->num << "," << v->num << ")\n";
            return 1;
        }
    }

    std::cerr << "PASS: more threads than input\n";
    return 0;
}
