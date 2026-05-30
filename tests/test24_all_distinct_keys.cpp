// test24_all_distinct_keys: 500 input pairs each mapping to a unique key.
// Assert 500 output groups each correct.
#include "MapReduceJob.h"
#include <iostream>
#include <vector>

class IntEl : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    int num;
    IntEl(int n) : num(n) {}
    bool operator<(const K1 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K2 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K3 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
};

class DistinctKeyClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        // emit (key, value) directly — each key is unique
        context.addIntermediate(std::dynamic_pointer_cast<IntEl>(key),
                                std::dynamic_pointer_cast<IntEl>(value));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        for (auto &p : pairs) {
            context.addOutput(std::dynamic_pointer_cast<K3>(p.first),
                              std::dynamic_pointer_cast<V3>(p.second));
        }
    }
};

int main() {
    DistinctKeyClient client;
    InputVec inputVec;
    const int N = 500;
    for (int i = 0; i < N; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(i * 3)});
    }

    MapReduceJob job(client, inputVec, 8);
    auto output = job.getOutput();

    if ((int)output.size() != N) {
        std::cerr << "FAIL: expected " << N << " output pairs, got " << output.size() << "\n";
        return 1;
    }

    // output is sorted by key (ascending)
    for (int i = 0; i < N; i++) {
        auto k = std::static_pointer_cast<IntEl>(output[i].first);
        auto v = std::static_pointer_cast<IntEl>(output[i].second);
        if (k->num != i || v->num != i * 3) {
            std::cerr << "FAIL: at index " << i << " expected (" << i << "," << i*3
                      << ") got (" << k->num << "," << v->num << ")\n";
            return 1;
        }
    }

    std::cerr << "PASS: 500 distinct keys\n";
    return 0;
}
