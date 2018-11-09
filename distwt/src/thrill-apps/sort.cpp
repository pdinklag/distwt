#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/wt.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>

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
    auto etext = ea.transform(rawtext).Cache();

    // construct wt
    const size_t height = WaveletTree::height(hist);

    WaveletTree wt(ctx, output);
    {
        auto text = etext;
        for(size_t level = 0; level < height; level++) {
            const size_t rsh = height - 1 - level;

            // compute and store BV
            wt.save_level_bv(level,
                text.Window(thrill::api::DisjointTag, 64,
                [rsh](size_t, const std::vector<esym_t>& v) {
                    uint64_t bv = 0;
                    for(size_t i = 0; i < v.size(); i++) {
                        // check level-th bit of symbol
                        if((v[i] >> rsh) & 1) {
                            bv |= (1ULL << (63ULL-i));
                        }
                    }
                    return bv;
                })
            );

            if(level+1 < height) {
                text = text.SortStable(
                    [rsh](esym_t a, esym_t b){
                        // stably sort according to newest bit
                        return (a >> rsh) < (b >> rsh);
                    }).Collapse();
            }
        }
    }

    // store histogram
    wt.save_histogram(hist);
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
