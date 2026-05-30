// test32_multiple_jobs_sequential_destroy.cpp
// Create several MapReduceJob objects with different inputs that run simultaneously
// (construct all, then getOutput each). Assert each yields its own correct sorted output
// (no cross-talk between jobs).
#include "MapReduceJob.h"
#include <iostream>
#include <vector>
#include <memory>

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

// map: emit (key, value); reduce: sum values per key
class SumClient : public MapReduceClient
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

static bool check_output(const OutputVec &out, int base, int n_keys)
{
    if (out.size() != static_cast<size_t>(n_keys)) {
        std::cerr << "FAIL: expected " << n_keys << " pairs, got " << out.size() << std::endl;
        return false;
    }
    for (size_t i = 0; i < out.size(); i++) {
        auto k = std::dynamic_pointer_cast<IntKey>(out[i].first);
        auto v = std::dynamic_pointer_cast<IntVal>(out[i].second);
        int expected_key = base + static_cast<int>(i);
        int expected_val = expected_key * 2; // we store val = key*2
        if (k->val != expected_key) {
            std::cerr << "FAIL: wrong key at index " << i
                      << ": expected " << expected_key << " got " << k->val << std::endl;
            return false;
        }
        if (v->val != expected_val) {
            std::cerr << "FAIL: wrong value for key=" << k->val
                      << ": expected " << expected_val << " got " << v->val << std::endl;
            return false;
        }
    }
    // Check sorted
    for (size_t i = 1; i < out.size(); i++) {
        if (*out[i].first < *out[i-1].first) {
            std::cerr << "FAIL: output not sorted at index " << i << std::endl;
            return false;
        }
    }
    return true;
}

int main()
{
    SumClient client;
    const int N_JOBS = 5;
    const int N_KEYS_PER_JOB = 80;

    // Build distinct input vectors with non-overlapping key ranges
    std::vector<InputVec> inputs(N_JOBS);
    for (int j = 0; j < N_JOBS; j++) {
        int base = j * N_KEYS_PER_JOB;
        for (int i = 0; i < N_KEYS_PER_JOB; i++) {
            int key = base + i;
            inputs[j].push_back({std::make_shared<IntKey>(key),
                                  std::make_shared<IntVal>(key * 2)});
        }
    }

    // Construct all jobs simultaneously (they start running immediately)
    std::vector<std::unique_ptr<MapReduceJob>> jobs;
    jobs.reserve(N_JOBS);
    for (int j = 0; j < N_JOBS; j++)
        jobs.push_back(std::make_unique<MapReduceJob>(client, inputs[j], 4));

    // Collect output and verify each job independently
    for (int j = 0; j < N_JOBS; j++) {
        OutputVec out = jobs[j]->getOutput();
        int base = j * N_KEYS_PER_JOB;
        if (!check_output(out, base, N_KEYS_PER_JOB)) {
            std::cerr << "(failure in job " << j << ")" << std::endl;
            return 1;
        }
    }

    // Destructors run as jobs go out of scope — they must block until done
    return 0;
}
