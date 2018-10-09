#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_binary.hpp>

#include <distwt/text.hpp>
#include <distwt/wt.hpp>
#include <distwt/histogram.hpp>
#include <distwt/effective_alphabet.hpp>

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

    bv_dia_t bv;
    for(size_t level = 0; level < wt_height; level++) {
        //text.Print(std::string("text_") + std::to_string(level+1));

        const size_t rsh = wt_height - 1 - level;

        // compute BV
        bv = text
            .Map([&](esym_t x) {
                // get level-th bit of symbol
                return bool((x >> rsh) & 1);
            })
            .Cache()
            .Execute(); // TODO: why is this required?

        //bv.Print(std::string("bv_") + std::to_string(level+1));
        wt_bits.emplace_back(bv);

        if(level+1 < wt_height) {
            text = text
                .SortStable(
                    [&](esym_t a, esym_t b){
                        // stably sort according to newest bit
                        return (a >> rsh) < (b >> rsh);
                    })
                .Execute() // NOW!
                .Collapse();
        }
    }
    return wt_bits;
}

void Process(thrill::Context& ctx, std::string input, std::string output) {
    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, input).Cache();

    // compute histogram
    Histogram hist(rawtext);

    // compute effective alphabet mapping
    auto eamap = compute_ea_map(hist);

    // transform text using effective alphabet
    auto etext = compute_effective_transformation(rawtext, eamap);

    // construct wt
    const size_t sigma = hist.size();
    auto wt_bits = ConstructWT_StableSort(etext, sigma);

    // write each level of the WT to file
    for(size_t i = 0; i < wt_bits.size(); i++) {
        wt_bits[i].WriteBinary(
            output + ".lv_" + std::to_string(i+1));
    }

    if(ctx.my_rank() == 0) {
        // write histogram to file
        hist.save(output + ".hist");
    }
}

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <input> <output>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], argv[2]);
    });
}
