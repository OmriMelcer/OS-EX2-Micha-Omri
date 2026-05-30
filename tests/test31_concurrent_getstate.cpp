// test31_concurrent_getstate.cpp
// While a job runs (map/reduce sleep briefly), spawn several threads
// hammering getState()/isDone() in bounded loops.
// Assert every observed state has stage in {0..3} and percentage in [0.0, 100.0].
// Detects torn/garbage reads from the atomic state encoding.
#include "MapReduceJob.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

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

// Client that sleeps a tiny bit in map so the job takes some time
class SlowClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        // Small sleep to ensure the job is "in flight" when observers check
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        auto k = std::dynamic_pointer_cast<IntKey>(key);
        auto v = std::dynamic_pointer_cast<IntVal>(value);
        ctx.addIntermediate(std::make_shared<IntKey>(k->val),
                            std::make_shared<IntVal>(v->val));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        auto k = std::dynamic_pointer_cast<IntKey>(pairs[0].first);
        int sum = 0;
        for (auto &p : pairs)
            sum += std::dynamic_pointer_cast<IntVal>(p.second)->val;
        ctx.addOutput(std::make_shared<IntKey>(k->val), std::make_shared<IntVal>(sum));
    }
};

int main()
{
    SlowClient client;
    InputVec input;
    for (int i = 0; i < 200; i++)
        input.push_back({std::make_shared<IntKey>(i), std::make_shared<IntVal>(i)});

    MapReduceJob job(client, input, 8);

    std::atomic<bool> found_bad{false};
    std::string bad_msg;

    // 6 observer threads, each polling up to 1,000,000 times or until job is done
    const int N_OBSERVERS = 6;
    const int MAX_ITERS = 1'000'000;
    std::vector<std::thread> observers;
    observers.reserve(N_OBSERVERS);

    for (int t = 0; t < N_OBSERVERS; t++) {
        observers.emplace_back([&job, &found_bad, &bad_msg]() {
            for (int iter = 0; iter < MAX_ITERS; iter++) {
                MapReduceState s = job.getState();
                int stage = static_cast<int>(s.stage);
                double pct = s.percentage;

                if (stage < 0 || stage > 3) {
                    // Only one thread should write the message (race is acceptable here
                    // since found_bad is atomic and we just want any one message)
                    if (!found_bad.exchange(true)) {
                        bad_msg = "stage out of range: " + std::to_string(stage);
                    }
                    return;
                }
                if (pct < -1e-6 || pct > 100.0 + 1e-6) {
                    if (!found_bad.exchange(true)) {
                        bad_msg = "percentage out of range: " + std::to_string(pct)
                                  + " at stage " + std::to_string(stage);
                    }
                    return;
                }

                // Also call isDone() — must not crash and must return bool
                (void)job.isDone();

                if (job.isDone())
                    break; // job finished, no more to check
            }
        });
    }

    for (auto &t : observers)
        t.join();

    job.wait(); // ensure job finished before we inspect

    if (found_bad.load()) {
        std::cerr << "FAIL: " << bad_msg << std::endl;
        return 1;
    }

    // Basic sanity on final output
    OutputVec out = job.getOutput();
    if (out.size() != 200) {
        std::cerr << "FAIL: expected 200 output pairs, got " << out.size() << std::endl;
        return 1;
    }

    return 0;
}
