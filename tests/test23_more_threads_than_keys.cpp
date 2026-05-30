// test23_more_threads_than_keys: N=32 but map produces only 2 distinct keys.
// Assert correct output, no hang.
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

// Each input pair emits key=(index%2), value=1. So only 2 distinct keys: 0 and 1.
class TwoKeyClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        auto k = std::static_pointer_cast<IntEl>(key);
        // Map to key 0 or 1
        context.addIntermediate(std::make_shared<IntEl>(k->num % 2),
                                std::make_shared<IntEl>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        auto key = std::static_pointer_cast<IntEl>(pairs[0].first);
        int count = (int)pairs.size();
        context.addOutput(std::make_shared<IntEl>(key->num),
                          std::make_shared<IntEl>(count));
    }
};

int main() {
    TwoKeyClient client;
    InputVec inputVec;
    // 20 input pairs: keys 0..19; each maps to key % 2
    for (int i = 0; i < 20; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(i)});
    }

    MapReduceJob job(client, inputVec, 32);
    auto output = job.getOutput();

    // Expect exactly 2 keys: 0 and 1, each with count 10
    if (output.size() != 2) {
        std::cerr << "FAIL: expected 2 output pairs, got " << output.size() << "\n";
        return 1;
    }
    auto k0 = std::static_pointer_cast<IntEl>(output[0].first);
    auto v0 = std::static_pointer_cast<IntEl>(output[0].second);
    auto k1 = std::static_pointer_cast<IntEl>(output[1].first);
    auto v1 = std::static_pointer_cast<IntEl>(output[1].second);
    if (k0->num != 0 || v0->num != 10) {
        std::cerr << "FAIL: key0: expected (0,10) got (" << k0->num << "," << v0->num << ")\n";
        return 1;
    }
    if (k1->num != 1 || v1->num != 10) {
        std::cerr << "FAIL: key1: expected (1,10) got (" << k1->num << "," << v1->num << ")\n";
        return 1;
    }

    std::cerr << "PASS: more threads than keys\n";
    return 0;
}
