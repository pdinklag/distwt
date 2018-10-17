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
void recursiveWT(
    WaveletTree& wt,
    const std::vector<size_t>& node_sizes,
    const float lower_threshold,
    const float upper_threshold,
    const size_t original_input_size,
    const size_t node_id,
    input_t input,
    const size_t a,
    const size_t b) {

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
        }));

    // "parallel split"
    auto balanced_recurse = [&](auto s, size_t id, size_t x, size_t y) {
        if(x < y) {
            const size_t child_sz = node_sizes[id-1];
            const float p = float(child_sz) / float(original_input_size);
            if(lower_threshold < p && p < upper_threshold) {
                LOG1 << "p = " << p << " for node " << id << " - rebalancing";
                recursiveWT(wt, node_sizes,
                    lower_threshold, upper_threshold, original_input_size,
                    id, s.Rebalance().Cache(), x, y);
            } else {
                recursiveWT(wt, node_sizes,
                    lower_threshold, upper_threshold, original_input_size,
                    id, s.Cache(), x, y);
            }
        }
    };

    balanced_recurse(
        input.Filter([m](const esym_t& c){ return (c <= m); }),
        2ULL * node_id, a, m);

    balanced_recurse(
        input.Filter([m](const esym_t& c){ return (c > m); }),
        2ULL * node_id + 1ULL, m+1, b);

}

void Process(
    thrill::Context& ctx,
    std::string input,
    size_t input_size,
    std::string output,
    const float lower_threshold,
    const float upper_threshold) {

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
    auto node_sizes = WaveletTree::node_sizes(hist);

    recursiveWT(
        wt,
        node_sizes,
        lower_threshold,
        upper_threshold,
        input_size,
        1ULL,             // start with root node
        etext,            // start with full transformed text
        0, WaveletTree::num_nodes(hist));
                          // start with [0, (2^h)-1] interval
                          // this is done to stay compatible with the other
                          // algorithms -> TODO: any negative side effects?

    // store additional information to disk
    wt.save_histogram(hist);
}

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <input> <output> [p] [q]" << std::endl;
        return -1;
    }

    float upper_threshold = (argc >= 4) ? std::stof(argv[3]) : 0.0f;
    float lower_threshold = (argc >= 5) ? std::stof(argv[4]) : 1.0f;

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], util::file_size(argv[1]), argv[2],
            lower_threshold, upper_threshold);
    });
}

