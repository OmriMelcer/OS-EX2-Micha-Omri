// test36_stress.cpp
// Large input (10000 pairs, many distinct keys), N=16 threads.
// Each key maps to itself with value 1; reduce sums all 1s per key
// (here each key appears twice — input has duplicate keys to create groups).
// Asserts correct aggregate result and sorted output.
// Primary target for valgrind/TSan scrutiny.
#include "MapReduceJob.h"
#include <iostream>
#include <vector>
#include <unordered_map>

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

// map: emit (key, 1) — counting occurrences
// reduce: sum all the 1s for this key
class CountClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> /*value*/,
             MapContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        ctx.addIntermediate(std::make_shared<IntKey>(k->val),
                            std::make_shared<IntVal>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int count = 0;
        for (auto &p : pairs)
            count += std::dynamic_pointer_cast<IntVal>(p.second)->val;
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(count));
    }
};

int main()
{
    CountClient client;
    const int N_UNIQUE_KEYS = 5000;
    const int REPS_PER_KEY  = 2;   // each key appears twice => expected count = 2
    const int TOTAL_INPUT   = N_UNIQUE_KEYS * REPS_PER_KEY; // 10000

    InputVec input;
    input.reserve(TOTAL_INPUT);
    // Insert in reverse-key order to stress the sort phase
    for (int rep = 0; rep < REPS_PER_KEY; rep++) {
        for (int i = N_UNIQUE_KEYS - 1; i >= 0; i--) {
            input.push_back({std::make_shared<IntKey>(i),
                             std::make_shared<IntVal>(0)}); // value unused
        }
    }

    MapReduceJob job(client, input, 16);
    OutputVec out = job.getOutput();

    // Expected: N_UNIQUE_KEYS entries, each with count = REPS_PER_KEY
    if (out.size() != static_cast<size_t>(N_UNIQUE_KEYS)) {
        std::cerr << "FAIL: expected " << N_UNIQUE_KEYS
                  << " output pairs, got " << out.size() << std::endl;
        return 1;
    }

    // Check sorted and correct counts
    for (size_t i = 0; i < out.size(); i++) {
        auto k = std::dynamic_pointer_cast<IntKey>(out[i].first);
        auto v = std::dynamic_pointer_cast<IntVal>(out[i].second);

        // Sorted check
        if (i > 0) {
            auto prev_k = std::dynamic_pointer_cast<IntKey>(out[i-1].first);
            // Compare via K3 base reference to avoid ambiguity
            const K3 &pk3 = *prev_k;
            const K3 &ck3 = *k;
            if (ck3 < pk3) {
                std::cerr << "FAIL: not sorted at index " << i
                          << ": key[" << (i-1) << "]=" << prev_k->val
                          << " > key[" << i << "]=" << k->val << std::endl;
                return 1;
            }
        }

        // Correct key value (should be 0..N_UNIQUE_KEYS-1 in order)
        if (k->val != static_cast<int>(i)) {
            std::cerr << "FAIL: unexpected key at index " << i
                      << ": expected " << i << " got " << k->val << std::endl;
            return 1;
        }

        // Correct count
        if (v->val != REPS_PER_KEY) {
            std::cerr << "FAIL: key=" << k->val
                      << " expected count=" << REPS_PER_KEY
                      << " got " << v->val << std::endl;
            return 1;
        }
    }

    return 0;
}
