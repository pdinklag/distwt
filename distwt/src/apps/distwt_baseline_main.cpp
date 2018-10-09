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
#include <thrill/api/window.hpp>

#include <distwt/text.hpp>
#include <distwt/util.hpp>
#include <distwt/binary_io.hpp>
#include <distwt/wt.hpp>

#include <distwt/histogram.hpp>
#include <distwt/effective_alphabet.hpp>

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
        bv = text.Window(thrill::api::DisjointTag, 64,
            [rsh](size_t, const std::vector<esym_t>& v) {
                uint64_t bv = 0;
                for(size_t i = 0; i < v.size(); i++) {
                    // get level-th bit of symbol
                    uint64_t bit = ((v[i] >> rsh) & 1) ? 1ULL : 0ULL;

                    // write into bit vector
                    bv |= (bit << (63ULL-i));
                }
                return bv;
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

void Process(
    thrill::Context& ctx,
    std::string input,
    size_t input_size,
    std::string output) {

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, input).Cache();

    // compute histogram
    Histogram hist(rawtext);

    // compute effective alphabet
    EffectiveAlphabet ea(hist);

    // transform text
    auto etext = ea.transform(rawtext);

    // construct wt
    const size_t sigma = hist.size();
    WaveletTree wt(input_size, hist, ConstructWT_StableSort(etext, sigma));
    wt.save(ctx, output);
}

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <input> <output>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], util::file_size(argv[1]), argv[2]);
    });
}
