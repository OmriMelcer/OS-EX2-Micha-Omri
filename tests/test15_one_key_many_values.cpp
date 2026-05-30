// test15_one_key_many_values.cpp
// All inputs map to the SAME k2 key (thousands of values).
// Assert exactly one reduced group with the correct aggregate.
#include "MapReduceJob.h"
#include <iostream>

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

// All inputs map to key=0; value is the original input index
// Reduce: sum all values
class OneKeyClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto v = std::dynamic_pointer_cast<IntVal>(value);
        // Always emit key=0, value=v->val
        ctx.addIntermediate(std::make_shared<IntKey>(0),
                            std::make_shared<IntVal>(v->val));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        long long sum = 0;
        for (auto &p : pairs)
            sum += std::dynamic_pointer_cast<IntVal>(p.second)->val;
        // Store sum in val (fits in int for our input size)
        ctx.addOutput(std::make_shared<IntKey>(k->val),
                      std::make_shared<IntVal>(static_cast<int>(sum)));
    }
};

int main()
{
    OneKeyClient client;
    InputVec input;
    const int N = 5000;
    // Insert N elements each with value i (0..N-1)
    for (int i = 0; i < N; i++)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

    MapReduceJob job(client, input, 8);
    job.wait();
    OutputVec out = job.getOutput();

    // Should have exactly ONE output pair (one key group)
    if (out.size() != 1) {
        std::cerr << "FAIL: expected 1 output pair, got " << out.size() << std::endl;
        return 1;
    }

    auto k = std::dynamic_pointer_cast<IntKey>(out[0].first);
    auto v = std::dynamic_pointer_cast<IntVal>(out[0].second);

    if (k->val != 0) {
        std::cerr << "FAIL: expected output key=0, got " << k->val << std::endl;
        return 1;
    }

    // Expected sum = 0+1+...+(N-1) = N*(N-1)/2
    int expectedSum = N * (N - 1) / 2;
    if (v->val != expectedSum) {
        std::cerr << "FAIL: expected sum=" << expectedSum << ", got " << v->val << std::endl;
        return 1;
    }

    return 0;
}
