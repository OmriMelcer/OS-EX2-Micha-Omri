// test10_output_sorted_n1.cpp
// N=1, deterministic int-key input; assert getOutput() is ascending by K3.
#include "MapReduceJob.h"
#include <iostream>
#include <vector>

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

// Maps (k,v) -> emits (k, v) as intermediate
class IdentityIntClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        auto v = std::dynamic_pointer_cast<IntVal>(value);
        ctx.addIntermediate(std::make_shared<IntKey>(k->val),
                            std::make_shared<IntVal>(v->val));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int sum = 0;
        for (auto &p : pairs)
            sum += std::dynamic_pointer_cast<IntVal>(p.second)->val;
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(sum));
    }
};

int main()
{
    IdentityIntClient client;
    InputVec input;
    // Insert in REVERSE order to expose any sort bug
    for (int i = 99; i >= 0; i--)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

    MapReduceJob job(client, input, 1);
    job.wait();
    OutputVec out = job.getOutput();

    if (out.empty()) {
        std::cerr << "FAIL: output is empty" << std::endl;
        return 1;
    }

    for (size_t i = 1; i < out.size(); i++) {
        // Check NOT (out[i] < out[i-1]), i.e. out[i] >= out[i-1]
        if (*out[i].first < *out[i-1].first) {
            auto prev = std::dynamic_pointer_cast<IntKey>(out[i-1].first);
            auto cur  = std::dynamic_pointer_cast<IntKey>(out[i].first);
            std::cerr << "FAIL: output not sorted at index " << i
                      << ": key[" << (i-1) << "]=" << prev->val
                      << " > key[" << i << "]=" << cur->val << std::endl;
            return 1;
        }
    }

    if (out.size() != 100) {
        std::cerr << "FAIL: expected 100 output pairs, got " << out.size() << std::endl;
        return 1;
    }

    return 0;
}
