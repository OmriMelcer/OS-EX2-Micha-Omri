// test34_getoutput_blocks_until_done.cpp
// Call getOutput() immediately after construction (no explicit wait()).
// Assert it returns only complete, correct, sorted output.
// The blocking nature of getOutput() means it must not return until all
// framework threads have finished reduce.
#include "MapReduceJob.h"
#include <iostream>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>

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

// Track reduce completions so we can verify all reduces ran before getOutput returned
static std::atomic<int> reduce_count{0};

class CountingClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        // Small sleep to ensure the job takes some time
        std::this_thread::sleep_for(std::chrono::microseconds(100));
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
        reduce_count.fetch_add(1, std::memory_order_relaxed);
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(sum));
    }
};

int main()
{
    CountingClient client;
    const int N_KEYS = 120;
    InputVec input;
    for (int i = N_KEYS - 1; i >= 0; i--)  // reverse order to expose sort bugs
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i * 3)});

    MapReduceJob job(client, input, 6);

    // Call getOutput() immediately — must block until complete
    OutputVec out = job.getOutput();

    // After getOutput returns, all N_KEYS reduces must have completed
    int r = reduce_count.load(std::memory_order_seq_cst);
    if (r != N_KEYS) {
        std::cerr << "FAIL: getOutput returned but only " << r << "/" << N_KEYS
                  << " reduces completed" << std::endl;
        return 1;
    }

    if (out.size() != static_cast<size_t>(N_KEYS)) {
        std::cerr << "FAIL: expected " << N_KEYS << " output pairs, got " << out.size() << std::endl;
        return 1;
    }

    // Verify sorted
    for (size_t i = 1; i < out.size(); i++) {
        if (*out[i].first < *out[i-1].first) {
            auto prev = std::dynamic_pointer_cast<IntKey>(out[i-1].first);
            auto cur  = std::dynamic_pointer_cast<IntKey>(out[i].first);
            std::cerr << "FAIL: not sorted at " << i
                      << ": keys " << prev->val << " > " << cur->val << std::endl;
            return 1;
        }
    }

    // Verify values
    for (size_t i = 0; i < out.size(); i++) {
        auto k = std::dynamic_pointer_cast<IntKey>(out[i].first);
        auto v = std::dynamic_pointer_cast<IntVal>(out[i].second);
        int expected_key = static_cast<int>(i);
        int expected_val = expected_key * 3;
        if (k->val != expected_key || v->val != expected_val) {
            std::cerr << "FAIL at index " << i
                      << ": key=" << k->val << " val=" << v->val
                      << " expected key=" << expected_key << " val=" << expected_val << std::endl;
            return 1;
        }
    }

    return 0;
}
