#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
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
void recursiveWT(
    WaveletTree& wt,
    const size_t node_id,
    input_t input,
    const size_t a,
    const size_t b) {

    if(a == b) return;

    const size_t m = (a + b) / 2;

    // compute node BV
    wt.save_node_bv(node_id,
        input.Window(thrill::api::DisjointTag, 64,
        [m](size_t, const std::vector<esym_t>& v) {
            uint64_t bv = 0;
            for(size_t i = 0; i < v.size(); i++) {
                if(v[i] > m) {
                    bv |= (1ULL << (63ULL-i));
                }
            }
            return bv;
        }), input.Keep().Size()); // TODO: compute node size using C array!

    // "parallel split"
    auto input_l = input.Filter([m](const esym_t& c){ return (c <= m); });
    auto input_r = input.Filter([m](const esym_t& c){ return (c >  m); });
    // TODO: heuristic for rebalancing based on node size?

    // left child
    recursiveWT(wt, 2ULL * node_id, input_l.Collapse(), a, m);

    // right child
    recursiveWT(wt, 2ULL * node_id + 1ULL, input_r.Collapse(), m+1, b);
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

    // construct wt recursively
    const size_t num_nodes = (1ULL << WaveletTree::height(hist)) - 1ULL;

    WaveletTree wt(ctx, output);
    recursiveWT(
        wt,
        1ULL,             // start with root node
        etext,            // start with full transformed text
        0, num_nodes);    // start with [0, (2^h)-1] interval
                          // this is done to stay compatible with the other
                          // algorithms -> TODO: any negative side effects?

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

