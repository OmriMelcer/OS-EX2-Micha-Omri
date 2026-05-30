// test35_destructor_blocks.cpp
// Construct a job in an inner scope with non-trivial work and let it go out
// of scope (destructor) WITHOUT calling wait()/getOutput(). Assert the program
// does not crash and that all reduce work actually completed before destruction
// returned (verified via an atomic side-effect counter).
#include "MapReduceJob.h"
#include <iostream>
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

static std::atomic<int> reduce_calls{0};
static const int N_KEYS = 80;

class SideEffectClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        // Moderate work to ensure destructor actually has something to wait for
        std::this_thread::sleep_for(std::chrono::microseconds(300));
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        auto v = std::dynamic_pointer_cast<IntVal>(value);
        ctx.addIntermediate(std::make_shared<IntKey>(k->val),
                            std::make_shared<IntVal>(v->val));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        reduce_calls.fetch_add(1, std::memory_order_relaxed);
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int sum = 0;
        for (auto &p : pairs)
            sum += std::dynamic_pointer_cast<IntVal>(p.second)->val;
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(sum));
    }
};

int main()
{
    SideEffectClient client;

    {
        InputVec input;
        for (int i = 0; i < N_KEYS; i++)
            input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

        // Construct job — it starts running immediately
        MapReduceJob job(client, input, 4);
        // Scope ends here: destructor is called WITHOUT any wait()/getOutput().
        // The destructor MUST block until all framework threads finish.
    }

    // After the scope exits (destructor returned), all reduces must be done
    int r = reduce_calls.load(std::memory_order_seq_cst);
    if (r != N_KEYS) {
        std::cerr << "FAIL: destructor returned but only " << r << "/" << N_KEYS
                  << " reduces completed" << std::endl;
        return 1;
    }

    return 0;
}
