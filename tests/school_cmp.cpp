/*
 * school_cmp.cpp — deterministic word-count over a larger fixed corpus.
 * Prints sorted "(key,count)" lines so both our framework and the school
 * solution can be diffed.
 * Named school_cmp.cpp so it does NOT match the test*.cpp glob and is
 * NOT picked up by run_tsan.sh / run_all.sh.
 */
#include "MapReduceJob.h"
#include <iostream>
#include <sstream>
#include <string>
#include <algorithm>

// ---- key/value types -------------------------------------------------------

class StringKey2 : public K2, public K3
{
public:
    std::string str;
    explicit StringKey2(const std::string &s) : str(s) {}
    bool operator<(const K2 &other) const override
    {
        return str < dynamic_cast<const StringKey2 &>(other).str;
    }
    bool operator<(const K3 &other) const override
    {
        return str < dynamic_cast<const StringKey2 &>(other).str;
    }
};

class IntVal : public V1, public V2, public V3
{
public:
    int num;
    explicit IntVal(int n) : num(n) {}
};

class SentenceVal : public V1
{
public:
    std::string sentence;
    explicit SentenceVal(const std::string &s) : sentence(s) {}
};

// ---- client ----------------------------------------------------------------

class WordCountClient2 : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> /*key*/, const std::shared_ptr<V1> value,
             MapContext &ctx) const override
    {
        auto sv = std::static_pointer_cast<SentenceVal>(value);
        std::istringstream iss(sv->sentence);
        std::string word;
        while (iss >> word)
        {
            // strip trailing punctuation
            while (!word.empty() && !isalpha((unsigned char)word.back()))
                word.pop_back();
            if (!word.empty())
                ctx.addIntermediate(std::make_shared<StringKey2>(word),
                                    std::make_shared<IntVal>(1));
        }
    }

    void reduce(const IntermediateVec &pairs, ReduceContext &ctx) const override
    {
        auto key = std::static_pointer_cast<StringKey2>(pairs[0].first);
        int count = 0;
        for (const auto &p : pairs)
            count += std::static_pointer_cast<IntVal>(p.second)->num;
        ctx.addOutput(std::make_shared<StringKey2>(key->str),
                      std::make_shared<IntVal>(count));
    }
};

// ---- corpus (fixed, deterministic) -----------------------------------------

static const char *CORPUS[] = {
    "the quick brown fox jumps over the lazy dog",
    "a quick brown dog outpaces a lazy fox",
    "the dog and the fox are friends in the forest",
    "quick foxes and lazy dogs live in harmony",
    "the forest is full of foxes dogs and birds",
    "birds fly over the forest and the mountains",
    "the mountains are covered with snow and ice",
    "ice and snow melt in the spring and rivers flow",
    "rivers flow through forests and valleys below",
    "the valley is full of flowers and trees and birds",
    "trees grow tall in the forest where foxes live",
    "foxes and dogs play in the snow near the mountains",
    "brown foxes jump over sleeping dogs in the park",
    "the park is a place where dogs and birds meet",
    "birds sing in the morning over the sleepy town",
    "the town wakes up to the sound of birds and rivers",
    "rivers bring water to the valley and the town",
    "the valley fills with flowers when snow melts away",
    "away from the city the forest remains quiet and still",
    "still water reflects the mountains and the trees above",
};

int main()
{
    WordCountClient2 client;
    InputVec input;
    for (const char *s : CORPUS)
        input.push_back({nullptr, std::make_shared<SentenceVal>(s)});

    MapReduceJob job(client, input, 6);
    OutputVec out = job.getOutput();

    // getOutput already sorts; print in that order
    for (const auto &p : out)
    {
        auto k = std::static_pointer_cast<StringKey2>(p.first);
        auto v = std::static_pointer_cast<IntVal>(p.second);
        std::cout << "(" << k->str << "," << v->num << ")\n";
    }
    return 0;
}
