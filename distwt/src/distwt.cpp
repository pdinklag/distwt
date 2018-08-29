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
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/zip.hpp>

using sym_t = unsigned char;
using esym_t = unsigned short;

using HistEntry = std::pair<sym_t, size_t>;
using EAMap = std::unordered_map<sym_t, esym_t>;

using wt_t = std::vector<thrill::DIA<bool>>;

std::ostream& operator << (std::ostream& os, const HistEntry& p) {
    return os << p.first << " -> " << p.second;
}

template<typename InputDIA>
wt_t ConstructWT_StableSort(const InputDIA& input, const size_t sigma) {

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
                        // stably sort according to newest bit
                        return (a >> rsh) < (b >> rsh);
                    })
                .Execute() // do it NOW
                .Collapse();
        }
    }

    return wt;
}

thrill::DIA<sym_t> DecodeWT(
    thrill::Context& ctx,
    const wt_t& wt,
    const std::vector<HistEntry>& hist) {

    using esym_index_t = std::pair<esym_t, size_t>;

    // reconstruct symbols from bit vector
    const size_t n = wt[0].Size();
    auto xtext = thrill::api::Generate(ctx, n, [](size_t i){
        // construct indexed sequence of 0 symbols
        return esym_index_t(0, i);}
    );

    const size_t wt_height = wt.size();
    for(size_t level = 0; level < wt_height; level++) {
        const size_t lsh = wt_height - 1 - level;

        xtext = wt[level]
            .Zip(xtext, [lsh](bool bit, esym_index_t x) {
                // OR symbols using current vector
                return esym_index_t(x.first | (bit << lsh), x.second);
            })
            .SortStable([lsh](esym_index_t a, esym_index_t b){
                // stably reorder according to newest bit
                return (a.first >> lsh) < (b.first >> lsh);
            })
            .Execute() // do it NOW
            .Collapse();
    }

    // restore original text
    auto text = xtext
        .SortStable([](esym_index_t a, esym_index_t b){
            // stably sort back by original index
            return a.second < b.second;
        })
        .Map([](esym_index_t x){
            // map indexed pair back to effective symbol
            return x.first;
        })
        .Map([hist](esym_t x){
            // undo effective transformation using histogram
            return hist[x].first;
        });

    return text.Cache();
}

template<typename ValueType>
size_t Compare(
    const thrill::DIA<ValueType>& a,
    const thrill::DIA<ValueType>& b) {

    return a
        .Zip(b, [](const ValueType& a, const ValueType& b){
            // test if single items are equal
            return (a != b) ? 1ULL : 0ULL;
        })
        .Sum([](size_t a, size_t b){
            // sum up amount of differing items (diff)
            return a + b;
        }, 0ULL);
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

    // decode WT
    auto decoded = DecodeWT(ctx, wt, hist);

    // compare raw and decoded texts as a means to verify the WT
    const size_t diff = Compare(rawtext, decoded);

    // output result on first worker
    if(ctx.my_rank() == 0) {
        if(diff == 0) {
            std::cout << "WT verification succeeded!" << std::endl;
        } else {
            std::cout << "WT verification FAILED (diff = " << diff << ")!" << std::endl;
        }
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

