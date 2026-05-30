// test14_multi_emit.cpp
// map emits MANY intermediate pairs per input element and reduce emits MULTIPLE
// (k3,v3) per group; assert total output count and per-key correctness.
//
// Setup: 5 input elements, each with value N (1..5).
// Map: for input element with value N, emit N copies of (key=N, value=1).
// Reduce: for group with key K having K*K pairs, emit K separate (K, i) for i in 1..K.
// Expected output: key K appears K times (for K in 1..5).
// Total output pairs: 1+2+3+4+5 = 15.
#include "MapReduceJob.h"
#include <iostream>
#include <map>

class IntKey : public K1, public K2, public K3
{
public:
    int val;
    explicit IntKey(int v) : val(v) {}
    bool operator<(const K1 &o) const override { return val < dynamic_cast<const IntKey&>(o).val; }
    bool operator<(const K2 &o) const override { return val < dynamic_cast<const IntKey&>(o).val; }
    bool operator<(const K3 &o) const override { return val < dynamic_cast<const IntKey&>(o).val; }
};

class IntVal : public V1, public V2, public V3
{
public:
    int val;
    explicit IntVal(int v) : val(v) {}
};

class MultiEmitClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        // Emit k->val copies of (key=k->val, value=1)
        for (int i = 0; i < k->val; i++)
            ctx.addIntermediate(std::make_shared<IntKey>(k->val),
                                std::make_shared<IntVal>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int count = static_cast<int>(pairs.size());
        // Emit 'count' output pairs: (key=k->val, value=i) for i in 1..count
        for (int i = 1; i <= count; i++)
            ctx.addOutput(std::make_shared<IntKey>(k->val),
                          std::make_shared<IntVal>(i));
    }
};

int main()
{
    MultiEmitClient client;
    InputVec input;
    for (int i = 1; i <= 5; i++)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

    MapReduceJob job(client, input, 4);
    job.wait();
    OutputVec out = job.getOutput();

    // Total pairs: 1+2+3+4+5 = 15
    if (out.size() != 15) {
        std::cerr << "FAIL: expected 15 output pairs, got " << out.size() << std::endl;
        return 1;
    }

    // Count occurrences of each key
    std::map<int,int> keyCounts;
    for (auto &p : out) {
        auto k = std::dynamic_pointer_cast<IntKey>(p.first);
        keyCounts[k->val]++;
    }

    // Key K should appear K times
    for (int k = 1; k <= 5; k++) {
        if (keyCounts[k] != k) {
            std::cerr << "FAIL: key " << k << " appears " << keyCounts[k]
                      << " times, expected " << k << std::endl;
            return 1;
        }
    }

    // Verify sorted order
    for (size_t i = 1; i < out.size(); i++) {
        if (*out[i].first < *out[i-1].first) {
            auto prev = std::dynamic_pointer_cast<IntKey>(out[i-1].first);
            auto cur  = std::dynamic_pointer_cast<IntKey>(out[i].first);
            std::cerr << "FAIL: output not sorted at index " << i
                      << ": key[" << (i-1) << "]=" << prev->val
                      << " > key[" << i << "]=" << cur->val << std::endl;
            return 1;
        }
    }

    return 0;
}
