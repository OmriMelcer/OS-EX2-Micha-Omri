// test21_single_pair: one input pair, N=4; assert correct single output.
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

class DoubleClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        auto k = std::static_pointer_cast<IntEl>(key);
        auto v = std::static_pointer_cast<IntEl>(value);
        // emit (key, value*2)
        context.addIntermediate(std::make_shared<IntEl>(k->num),
                                std::make_shared<IntEl>(v->num * 2));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        for (auto &p : pairs)
            context.addOutput(std::dynamic_pointer_cast<K3>(p.first),
                              std::dynamic_pointer_cast<V3>(p.second));
    }
};

int main() {
    DoubleClient client;
    InputVec inputVec;
    inputVec.push_back({std::make_shared<IntEl>(7), std::make_shared<IntEl>(21)});

    MapReduceJob job(client, inputVec, 4);
    auto output = job.getOutput();

    if (output.size() != 1) {
        std::cerr << "FAIL: expected 1 output pair, got " << output.size() << "\n";
        return 1;
    }
    auto k = std::static_pointer_cast<IntEl>(output[0].first);
    auto v = std::static_pointer_cast<IntEl>(output[0].second);
    if (k->num != 7 || v->num != 42) {
        std::cerr << "FAIL: expected (7,42) got (" << k->num << "," << v->num << ")\n";
        return 1;
    }

    std::cerr << "PASS: single pair\n";
    return 0;
}
