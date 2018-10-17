#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>

#include <distwt/text.hpp>
#include <distwt/util.hpp>
#include <distwt/wt.hpp>

#include <distwt/binary_io.hpp>
#include <distwt/histogram.hpp>
#include <distwt/effective_alphabet.hpp>

template<typename input_t>
void flatWT(
    WaveletTree& wt,
    const Histogram& hist,
    input_t input) {

    auto node_sizes = WaveletTree::node_sizes(hist);
    const size_t height = WaveletTree::height(hist);

    // proceed level by level
    size_t node_id = 1ULL;
    for(size_t level = 0; level < height; level++) {
        const size_t rsh = height - 1ULL - level;
        const size_t pref = rsh + 1ULL;

        const size_t level_nodes = (1ULL << level);
        for(size_t i = 0; i < level_nodes; i++) {
            // for a symbol to go in this node, its bit prefix of length pref
            // has to be precisely i
            wt.save_node_bv(node_id++,
                input
                .Filter([pref, i](const esym_t& c){ return (c >> pref) == i; })
                .Window(thrill::api::DisjointTag, 64,
                [rsh](size_t, const std::vector<esym_t>& v) {
                    uint64_t bv = 0;
                    for(size_t i = 0; i < v.size(); i++) {
                        // check level-th bit of symbol
                        if((v[i] >> rsh) & 1) {
                            bv |= (1ULL << (63ULL-i));
                        }
                    }
                    return bv;
                }));
        }
    }
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
    auto etext = ea.transform(rawtext).Cache();

    // construct wt recursively
    WaveletTree wt(ctx, output);
    flatWT(wt, hist, etext);

    // store additional information to disk
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

