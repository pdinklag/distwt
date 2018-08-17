#include <functional>
#include <iostream>
#include <tuple>
#include <unordered_map>

#include <tlx/math/div_ceil.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/zip.hpp>

using sym_t = unsigned char;
using esym_t = unsigned short;

using HistEntry = std::pair<sym_t, size_t>;
using EAMap = std::unordered_map<sym_t, esym_t>;

std::ostream& operator << (std::ostream& os, const HistEntry& p) {
    return os << p.first << " -> " << p.second;
}

void ConstructWTNode(size_t id, const thrill::DIA<esym_t>& s, size_t a, size_t b) {
    const auto m = (a+b)/2;
    const auto node_name = std::string("node_") + std::to_string(id);

    auto bv = s.Map([&](esym_t x){ return (x > m); });
    bv.Print(node_name);
    bv.Collapse();

    if(a != b) {
        auto left = s.Filter([&](esym_t x){ return (x <= m); }).Cache();
        ConstructWTNode(2*id, left, a, m);
        left.Collapse();

        auto right = s.Filter([&](esym_t x){ return (x > m); }).Cache();
        ConstructWTNode(2*id+1, right, m+1, b);
        right.Collapse();
    }
}

void Process(thrill::Context& ctx, std::string input) {
    // load text
    auto rawtext = thrill::api::ReadBinary<sym_t>(ctx, input).Cache();
    const size_t n = rawtext.Size();

    // compute histogram
    auto hist = rawtext
        .Map([](sym_t x){ return HistEntry(x, 1); })
        .ReduceByKey(
            [](const HistEntry& e){ return e.first; }, // key extractor
            [](const HistEntry& a, const HistEntry& b){ return HistEntry(a.first, a.second+b.second); } // reduce
        )
        .Sort([](const HistEntry& a, const HistEntry& b){ return a.first < b.first; })
        .AllGather();

    // compute effective alphabet mapping
    const size_t sigma = hist.size();

    EAMap eamap;
    for(size_t i = 0; i < hist.size(); i++) {
        eamap.emplace(hist[i].first, esym_t(i));
    }

    // transform text using effective alphabet
    auto text = rawtext.Map([&](sym_t x){ return eamap[x]; }).Cache();

    // TODO: rawtext is no longer needed from here!

    // debug
    text.Print("text");

    // construct WT recursively
    ConstructWTNode(1, text, 0, sigma-1);
}

int main(int argc, const char** argv) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <input>" << std::endl;
        return -1;
    }

    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1]);
    });
}

