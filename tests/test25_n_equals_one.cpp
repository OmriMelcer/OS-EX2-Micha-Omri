// test25_n_equals_one: N=1 full end-to-end correctness (exercises barrier(1) path).
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

class SumClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        auto k = std::static_pointer_cast<IntEl>(key);
        // group into buckets of 10
        context.addIntermediate(std::make_shared<IntEl>(k->num / 10),
                                std::make_shared<IntEl>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        auto key = std::static_pointer_cast<IntEl>(pairs[0].first);
        context.addOutput(std::make_shared<IntEl>(key->num),
                          std::make_shared<IntEl>((int)pairs.size()));
    }
};

int main() {
    SumClient client;
    InputVec inputVec;
    // 30 input pairs: 0..29 -> bucket 0 (10 items), 1 (10 items), 2 (10 items)
    for (int i = 0; i < 30; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(i)});
    }

    MapReduceJob job(client, inputVec, 1);
    auto output = job.getOutput();

    if (output.size() != 3) {
        std::cerr << "FAIL: expected 3 output groups, got " << output.size() << "\n";
        return 1;
    }
    for (int i = 0; i < 3; i++) {
        auto k = std::static_pointer_cast<IntEl>(output[i].first);
        auto v = std::static_pointer_cast<IntEl>(output[i].second);
        if (k->num != i || v->num != 10) {
            std::cerr << "FAIL: bucket " << i << ": expected (" << i << ",10) got ("
                      << k->num << "," << v->num << ")\n";
            return 1;
        }
    }

    std::cerr << "PASS: N=1\n";
    return 0;
}
