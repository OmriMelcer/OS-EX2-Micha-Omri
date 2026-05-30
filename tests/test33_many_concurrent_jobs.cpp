// test33_many_concurrent_jobs.cpp
// Launch 10 jobs at once, each on its own std::thread (construct + getOutput inside thread).
// Join all threads; assert each result is correct. Stresses independence of per-job state.
#include "MapReduceJob.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

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

int main()
{
    const int N_JOBS = 10;
    const int N_KEYS = 50;

    SumClient client;
    std::atomic<int> failures{0};

    // Each thread creates its own job with a unique key range and validates output
    std::vector<std::thread> threads;
    threads.reserve(N_JOBS);

    for (int j = 0; j < N_JOBS; j++) {
        threads.emplace_back([j, N_KEYS, &client, &failures]() {
            int base = j * N_KEYS;
            InputVec input;
            for (int i = 0; i < N_KEYS; i++) {
                int key = base + i;
                input.push_back({std::make_shared<IntKey>(key),
                                 std::make_shared<IntVal>(key)});
            }

            MapReduceJob job(client, input, 4);
            OutputVec out = job.getOutput();

            if (out.size() != static_cast<size_t>(N_KEYS)) {
                std::cerr << "FAIL[job " << j << "]: expected " << N_KEYS
                          << " pairs, got " << out.size() << std::endl;
                failures.fetch_add(1);
                return;
            }

            // Verify sorted and values correct
            for (size_t i = 1; i < out.size(); i++) {
                if (*out[i].first < *out[i-1].first) {
                    std::cerr << "FAIL[job " << j << "]: output not sorted at index " << i << std::endl;
                    failures.fetch_add(1);
                    return;
                }
            }
            for (size_t i = 0; i < out.size(); i++) {
                auto k = std::dynamic_pointer_cast<IntKey>(out[i].first);
                auto v = std::dynamic_pointer_cast<IntVal>(out[i].second);
                int expected_key = base + static_cast<int>(i);
                if (k->val != expected_key || v->val != expected_key) {
                    std::cerr << "FAIL[job " << j << "]: wrong key/val at index " << i
                              << ": key=" << k->val << " val=" << v->val
                              << " expected both=" << expected_key << std::endl;
                    failures.fetch_add(1);
                    return;
                }
            }
        });
    }

    for (auto &t : threads)
        t.join();

    if (failures.load() > 0) {
        std::cerr << "FAIL: " << failures.load() << " job(s) reported errors" << std::endl;
        return 1;
    }

    return 0;
}
