// test26_state_progression: verify stage only advances 0->1->2->3, percentage in [0,100],
// and at isDone() stage==REDUCE with percentage==100.
#include "MapReduceJob.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>

class IntEl : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    int num;
    IntEl(int n) : num(n) {}
    bool operator<(const K1 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K2 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K3 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
};

class SlowClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto k = std::static_pointer_cast<IntEl>(key);
        context.addIntermediate(std::make_shared<IntEl>(k->num % 5),
                                std::make_shared<IntEl>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto key = std::static_pointer_cast<IntEl>(pairs[0].first);
        context.addOutput(std::make_shared<IntEl>(key->num),
                          std::make_shared<IntEl>((int)pairs.size()));
    }
};

int main() {
    SlowClient client;
    InputVec inputVec;
    for (int i = 0; i < 50; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(i)});
    }

    MapReduceJob job(client, inputVec, 4);

    int prev_stage = 0; // UNDEFINED_STAGE
    bool stage_went_backward = false;
    bool pct_out_of_range = false;

    // Poll bounded: up to 10000 iterations of 1ms = 10 seconds max
    int iterations = 0;
    const int MAX_ITER = 10000;
    while (iterations < MAX_ITER) {
        auto state = job.getState();
        int cur_stage = (int)state.stage;
        double pct = state.percentage;

        if (pct < 0.0 || pct > 100.0) {
            std::cerr << "FAIL: percentage out of range: " << pct
                      << " at stage " << cur_stage << "\n";
            pct_out_of_range = true;
            break;
        }
        if (cur_stage < prev_stage) {
            std::cerr << "FAIL: stage went backward from " << prev_stage
                      << " to " << cur_stage << "\n";
            stage_went_backward = true;
            break;
        }
        prev_stage = cur_stage;

        if (job.isDone()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        iterations++;
    }

    if (stage_went_backward || pct_out_of_range) return 1;

    if (iterations >= MAX_ITER) {
        std::cerr << "FAIL: job did not complete within " << MAX_ITER << "ms\n";
        return 1;
    }

    // After isDone(), verify final state
    job.wait();
    auto final_state = job.getState();
    if (final_state.stage != REDUCE_STAGE) {
        std::cerr << "FAIL: after isDone(), stage=" << final_state.stage
                  << " expected REDUCE_STAGE(3)\n";
        return 1;
    }
    if (final_state.percentage < 99.9999) {
        std::cerr << "FAIL: after isDone(), percentage=" << final_state.percentage
                  << " expected 100\n";
        return 1;
    }

    std::cerr << "PASS: state progression valid\n";
    return 0;
}
