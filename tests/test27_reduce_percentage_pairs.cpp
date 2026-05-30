// test27_reduce_percentage_pairs: uneven reduce groups (1 key with 1000 values, 10 keys with 1 value).
// Poll reduce percentage; assert monotonic non-decreasing and ends exactly at 100.
// Catches reduce-progress denominator counting vectors instead of pairs.
#include "MapReduceJob.h"
#include <iostream>
#include <chrono>
#include <thread>

class IntEl : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    int num;
    IntEl(int n) : num(n) {}
    bool operator<(const K1 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K2 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
    bool operator<(const K3 &o) const override { return num < dynamic_cast<const IntEl&>(o).num; }
};

// Map: first 1000 inputs -> key 0 (big group); next 10 inputs -> keys 1..10 (singleton groups)
class UnevenClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override {
        auto k = std::static_pointer_cast<IntEl>(key);
        // k->num: 0..999 go to key 0; 1000..1009 go to keys 1..10
        int out_key = (k->num < 1000) ? 0 : (k->num - 999);
        context.addIntermediate(std::make_shared<IntEl>(out_key),
                                std::make_shared<IntEl>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        auto key = std::static_pointer_cast<IntEl>(pairs[0].first);
        context.addOutput(std::make_shared<IntEl>(key->num),
                          std::make_shared<IntEl>((int)pairs.size()));
    }
};

int main() {
    UnevenClient client;
    InputVec inputVec;
    for (int i = 0; i < 1010; i++) {
        inputVec.push_back({std::make_shared<IntEl>(i), std::make_shared<IntEl>(1)});
    }

    MapReduceJob job(client, inputVec, 4);

    double prev_reduce_pct = -1.0;
    bool monotonic_violated = false;
    double last_reduce_pct = -1.0;
    bool saw_reduce = false;

    // Bounded poll: up to 15000ms
    for (int i = 0; i < 15000 && !job.isDone(); i++) {
        auto state = job.getState();
        if (state.stage == REDUCE_STAGE) {
            saw_reduce = true;
            double pct = state.percentage;
            if (pct < prev_reduce_pct - 1e-6) {
                std::cerr << "FAIL: reduce percentage went backward: "
                          << prev_reduce_pct << " -> " << pct << "\n";
                monotonic_violated = true;
                break;
            }
            prev_reduce_pct = pct;
            last_reduce_pct = pct;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (monotonic_violated) return 1;

    if (!job.isDone()) {
        std::cerr << "FAIL: job did not complete within 15000ms\n";
        return 1;
    }

    job.wait();
    auto final_state = job.getState();
    if (std::abs(final_state.percentage - 100.0) > 1e-6) {
        std::cerr << "FAIL: final reduce percentage=" << final_state.percentage
                  << " expected 100.0\n";
        return 1;
    }
    if (final_state.stage != REDUCE_STAGE) {
        std::cerr << "FAIL: final stage=" << final_state.stage << " expected REDUCE\n";
        return 1;
    }

    // Check correctness of output
    auto output = job.getOutput();
    if (output.size() != 11) {
        std::cerr << "FAIL: expected 11 output pairs, got " << output.size() << "\n";
        return 1;
    }
    // key 0 should have count 1000
    auto k0 = std::static_pointer_cast<IntEl>(output[0].first);
    auto v0 = std::static_pointer_cast<IntEl>(output[0].second);
    if (k0->num != 0 || v0->num != 1000) {
        std::cerr << "FAIL: key0 expected (0,1000), got (" << k0->num << "," << v0->num << ")\n";
        return 1;
    }
    // keys 1..10 should have count 1 each
    for (int i = 1; i <= 10; i++) {
        auto k = std::static_pointer_cast<IntEl>(output[i].first);
        auto v = std::static_pointer_cast<IntEl>(output[i].second);
        if (k->num != i || v->num != 1) {
            std::cerr << "FAIL: key" << i << " expected (" << i << ",1) got ("
                      << k->num << "," << v->num << ")\n";
            return 1;
        }
    }

    std::cerr << "PASS: reduce percentage monotonic and ends at 100\n";
    return 0;
}
