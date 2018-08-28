#include <functional>
#include <iostream>
#include <tuple>
#include <unordered_map>

#include <tlx/math/integer_log2.hpp>

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

template<typename InputDIA>
std::vector<thrill::DIA<bool>> ConstructWT_StableSort(
    const InputDIA& input, const size_t sigma) {

    // compute size of WT
    const size_t wt_height = tlx::integer_log2_ceil(sigma-1);

    // construct WT level by level using stable sorting approach
    auto text = input.Collapse();
    std::vector<thrill::DIA<bool>> wt;

    for(size_t level = 0; level < wt_height; level++) {
        const size_t rsh = wt_height - 1 - level;

        // compute BV
        auto bv = text
            .Map([&](esym_t x) {
                // get level-th bit of symbol
                return bool((x >> rsh) & 1);
            });

        wt.push_back(bv.Cache()); // TODO: why does it ONLY work with Cache? (but not with Collapse, Execute, Keep, ...)

        if(level+1 < wt_height) {
            text = text
                .SortStable(
                    [&](esym_t a, esym_t b){
                        return (a >> rsh) < (b >> rsh);
                    })
                .Execute() // do it NOW
                .Collapse();
        }
    }

    return wt;
}

void Process(thrill::Context& ctx, std::string input) {

    // load raw text
    auto rawtext = thrill::api::ReadBinary<sym_t>(ctx, input).Cache();

    // compute histogram
    auto hist = rawtext
        .Map([](sym_t x){ return HistEntry(x, 1); })
        .ReduceByKey(
            [](const HistEntry& e){ return e.first; }, // key extractor
            [](const HistEntry& a, const HistEntry& b){ return HistEntry(a.first, a.second+b.second); } // reduce
        )
        .Sort([](const HistEntry& a, const HistEntry& b){ return a.first < b.first; })
        .AllGather();

    // read alphabet size
    const size_t sigma = hist.size();

    // compute effective alphabet mapping
    EAMap eamap;
    for(size_t i = 0; i < hist.size(); i++) {
        eamap.emplace(hist[i].first, esym_t(i));
    }

    // TODO: output eamap to file?

    // transform text using effective alphabet
    auto text = rawtext.Map([&](sym_t x){ return eamap[x]; }).Execute();

    // construct wt
    auto wt = ConstructWT_StableSort(text, sigma);

    // print WT
    for(size_t i = 0; i < wt.size(); i++) {
        wt[i].Print(std::string("wt_") + std::to_string(i+1));
    }
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

