// test30_concurrent_wait.cpp
// Construct one job; spawn 8 threads that all call job.wait() concurrently;
// then assert isDone() and correct output. Verifies wait() is thread-safe
// and internally joins each framework thread exactly once.
#include "MapReduceJob.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

// ---- key/value types ----
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

// Identity map/reduce: each key maps to itself with value sum
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
    const int N_KEYS = 100;
    SumClient client;
    InputVec input;
    for (int i = 0; i < N_KEYS; i++)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

    MapReduceJob job(client, input, 4);

    // 8 threads all racing to call wait()
    const int N_WAITERS = 8;
    std::vector<std::thread> waiters;
    waiters.reserve(N_WAITERS);
    for (int i = 0; i < N_WAITERS; i++)
        waiters.emplace_back([&job]() { job.wait(); });

    for (auto &t : waiters)
        t.join();

    // After all wait() calls return, job must be done
    if (!job.isDone()) {
        std::cerr << "FAIL: isDone() returned false after all wait() calls returned" << std::endl;
        return 1;
    }

    OutputVec out = job.getOutput();
    if (out.size() != static_cast<size_t>(N_KEYS)) {
        std::cerr << "FAIL: expected " << N_KEYS << " output pairs, got " << out.size() << std::endl;
        return 1;
    }

    // Verify sorted and correct values
    for (size_t i = 1; i < out.size(); i++) {
        if (*out[i].first < *out[i-1].first) {
            std::cerr << "FAIL: output not sorted at index " << i << std::endl;
            return 1;
        }
    }
    for (size_t i = 0; i < out.size(); i++) {
        auto k = std::dynamic_pointer_cast<IntKey>(out[i].first);
        auto v = std::dynamic_pointer_cast<IntVal>(out[i].second);
        if (k->val != v->val) {
            std::cerr << "FAIL: key=" << k->val << " has unexpected value=" << v->val << std::endl;
            return 1;
        }
    }

    return 0;
}
