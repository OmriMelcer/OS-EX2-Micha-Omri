// test11_output_sorted_multi.cpp
// N=8, many keys; assert getOutput() is ascending by K3.
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
    // 500 keys in shuffled (reverse) order to expose sort bugs with multi-threading
    const int N_KEYS = 500;
    for (int i = N_KEYS - 1; i >= 0; i--)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i * 2)});

    MapReduceJob job(client, input, 8);
    job.wait();
    OutputVec out = job.getOutput();

    if (out.size() != static_cast<size_t>(N_KEYS)) {
        std::cerr << "FAIL: expected " << N_KEYS << " output pairs, got " << out.size() << std::endl;
        return 1;
    }

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
