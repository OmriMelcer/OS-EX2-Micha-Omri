// test13_value_aggregation.cpp
// Word-count style with KNOWN expected counts (hardcoded corpus).
// Asserts every (word,count) matches exactly and no key is missing/extra.
#include "MapReduceJob.h"
#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <vector>

class StringKey : public K2, public K3
{
public:
    std::string str;
    explicit StringKey(const std::string &s) : str(s) {}
    bool operator<(const K2 &o) const override { return str < dynamic_cast<const StringKey&>(o).str; }
    bool operator<(const K3 &o) const override { return str < dynamic_cast<const StringKey&>(o).str; }
};

class StringValue : public V1
{
public:
    std::string str;
    explicit StringValue(const std::string &s) : str(s) {}
};

class IntVal : public V2, public V3
{
public:
    int val;
    explicit IntVal(int v) : val(v) {}
};

class WordCountClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto sv = std::static_pointer_cast<StringValue>(value);
        std::istringstream iss(sv->str);
        std::string word;
        while (iss >> word)
            ctx.addIntermediate(std::make_shared<StringKey>(word),
                                std::make_shared<IntVal>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto k = std::static_pointer_cast<StringKey>(pairs[0].first);
        int count = 0;
        for (auto &p : pairs)
            count += std::static_pointer_cast<IntVal>(p.second)->val;
        ctx.addOutput(std::make_shared<StringKey>(k->str), std::make_shared<IntVal>(count));
    }
};

int main()
{
    WordCountClient client;

    // Hardcoded corpus - compute expected counts manually
    std::vector<std::string> sentences = {
        "apple banana cherry apple",
        "banana cherry date apple apple",
        "cherry date elderberry cherry",
        "date elderberry fig date date",
        "apple fig fig fig",
    };

    // Expected counts computed by hand:
    // apple:  1+2+2 = 5 (sentence 0:1, sentence 1:2, sentence 4:1... let me recount)
    // apple: s0=1, s1=2, s4=1 → but sentence 2 has no apple...
    // s0: apple(1) banana(1) cherry(1) apple(1) → apple=2
    // s1: banana(1) cherry(1) date(1) apple(1) apple(1) → apple=2
    // s2: cherry(1) date(1) elderberry(1) cherry(1) → apple=0
    // s3: date(1) elderberry(1) fig(1) date(1) date(1) → apple=0
    // s4: apple(1) fig(1) fig(1) fig(1) → apple=1
    // apple total: 2+2+0+0+1 = 5
    // banana: s0=1, s1=1 = 2
    // cherry: s0=1, s1=1, s2=2 = 4
    // date: s1=1, s2=1, s3=3 = 5
    // elderberry: s2=1, s3=1 = 2
    // fig: s3=1, s4=3 = 4
    std::map<std::string, int> expected = {
        {"apple",      5},
        {"banana",     2},
        {"cherry",     4},
        {"date",       5},
        {"elderberry", 2},
        {"fig",        4},
    };

    InputVec input;
    for (auto &s : sentences)
        input.push_back({nullptr, std::make_shared<StringValue>(s)});

    MapReduceJob job(client, input, 4);
    job.wait();
    OutputVec out = job.getOutput();

    if (out.size() != expected.size()) {
        std::cerr << "FAIL: expected " << expected.size() << " output keys, got " << out.size() << std::endl;
        return 1;
    }

    std::map<std::string, int> got;
    for (auto &p : out) {
        auto k = std::static_pointer_cast<StringKey>(p.first);
        auto v = std::static_pointer_cast<IntVal>(p.second);
        got[k->str] = v->val;
    }

    for (auto &[word, count] : expected) {
        if (got.find(word) == got.end()) {
            std::cerr << "FAIL: missing key '" << word << "'" << std::endl;
            return 1;
        }
        if (got[word] != count) {
            std::cerr << "FAIL: word '" << word << "': expected " << count
                      << " got " << got[word] << std::endl;
            return 1;
        }
    }

    for (auto &[word, count] : got) {
        if (expected.find(word) == expected.end()) {
            std::cerr << "FAIL: unexpected key '" << word << "' with count " << count << std::endl;
            return 1;
        }
    }

    return 0;
}
