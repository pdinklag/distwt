#include <iostream>
#include <tuple>

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

#include "def.hpp"
#include "histogram.hpp"
#include "effective_alphabet.hpp"
#include "dia_compare.hpp"

// DIA type used for distributed bit vectors
using bv_dia_t = thrill::DIA<bool>;

// storage for a wavelet tree's bit vector
using wt_bits_t = std::vector<bv_dia_t>;

wt_bits_t ConstructWT_StableSort(const etext_t& input, const size_t sigma) {

    // compute size of WT
    const size_t wt_height = tlx::integer_log2_ceil(sigma-1);

    // construct WT level by level using stable sorting approach
    auto text = input;
    wt_bits_t wt_bits;

    for(size_t level = 0; level < wt_height; level++) {
        //text.Print(std::string("text_") + std::to_string(level+1));

        const size_t rsh = wt_height - 1 - level;

        // compute BV
        auto bv = text
            .Map([&](esym_t x) {
                // get level-th bit of symbol
                return bool((x >> rsh) & 1);
            });
        bv.Print(std::string("bv_") + std::to_string(level+1));
        wt_bits.push_back(bv.Cache()); // FIXME: this doesn't work as intended...

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

    return wt_bits;
}

thrill::DIA<rawsym_t> DecodeWT(
    thrill::Context& ctx,
    const wt_bits_t& wt_bits,
    const hist_t& hist) {

    using esym_index_t = std::pair<esym_t, size_t>;

    // reconstruct symbols from bit vector
    const size_t n = wt_bits[0].Size();
    auto xtext = thrill::api::Generate(ctx, n, [](size_t i){
        // construct indexed sequence of 0 symbols
        return esym_index_t(0, i);}
    );

    const size_t wt_height = wt_bits.size();
    for(size_t level = 0; level < wt_height; level++) {
        const size_t lsh = wt_height - 1 - level;

        xtext = wt_bits[level]
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

void Process(thrill::Context& ctx, std::string input) {

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, input).Cache();

    // compute histogram
    auto hist = compute_histogram(rawtext);

    // compute effective alphabet mapping
    auto eamap = compute_ea_map(hist);

    // transform text using effective alphabet
    auto etext = compute_effective_transformation(rawtext, eamap);

    // construct wt
    const size_t sigma = hist.size();
    auto wt_bits = ConstructWT_StableSort(etext, sigma);

    // print WT
    for(size_t i = 0; i < wt_bits.size(); i++) {
        wt_bits[i].Print(std::string("wt_bits_") + std::to_string(i+1));
    }

    // decode WT
    auto decoded = DecodeWT(ctx, wt_bits, hist);

    // compare raw and decoded texts as a means to verify the WT
    const size_t diff = dia_compare(rawtext, decoded);

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

