// test12_thread_invariance.cpp
// Run the same job logic at N in {1,2,4,16,64,100}; assert all produce identical
// sorted (key,value) multisets.
#include "MapReduceJob.h"
#include <iostream>
#include <vector>
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

// Word-count style: each input key->value is (number, 1);
// intermediate: emit (number % 10, 1) to merge into 10 buckets
class BucketCountClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        // bucket = k->val % 10
        ctx.addIntermediate(std::make_shared<IntKey>(k->val % 10),
                            std::make_shared<IntVal>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int count = static_cast<int>(pairs.size());
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(count));
    }
};

// Build a canonical reference: map from key->value
using KVMap = std::map<int,int>;

KVMap extractKV(const OutputVec &out)
{
    KVMap m;
    for (auto &p : out) {
        auto k = std::dynamic_pointer_cast<IntKey>(p.first);
        auto v = std::dynamic_pointer_cast<IntVal>(p.second);
        m[k->val] = v->val;
    }
    return m;
}

int main()
{
    BucketCountClient client;
    InputVec input;
    // 100 elements: 0..99, each bucket 0..9 should have count=10
    for (int i = 0; i < 100; i++)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(1)});

    std::vector<int> threadCounts = {1, 2, 4, 16, 64, 100};

    KVMap reference;
    bool haveReference = false;

    for (int n : threadCounts) {
        MapReduceJob job(client, input, n);
        job.wait();
        OutputVec out = job.getOutput();
        KVMap kv = extractKV(out);

        if (!haveReference) {
            reference = kv;
            haveReference = true;
        } else {
            if (kv != reference) {
                std::cerr << "FAIL: output differs for N=" << n
                          << " vs N=" << threadCounts[0] << std::endl;
                std::cerr << "  Expected " << reference.size() << " keys, got " << kv.size() << std::endl;
                for (auto &[k,v] : reference) {
                    if (kv.find(k) == kv.end())
                        std::cerr << "  Missing key " << k << std::endl;
                    else if (kv[k] != v)
                        std::cerr << "  Key " << k << ": expected " << v << " got " << kv[k] << std::endl;
                }
                return 1;
            }
        }
    }

    // Also verify the expected counts (10 per bucket)
    if (reference.size() != 10) {
        std::cerr << "FAIL: expected 10 buckets, got " << reference.size() << std::endl;
        return 1;
    }
    for (auto &[k, v] : reference) {
        if (v != 10) {
            std::cerr << "FAIL: bucket " << k << " has count " << v << ", expected 10" << std::endl;
            return 1;
        }
    }

    return 0;
}
