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
#include <thrill/api/write_binary.hpp>

#include <distwt/text.hpp>
#include <distwt/util.hpp>
#include <distwt/wt.hpp>

#include <distwt/binary_io.hpp>
#include <distwt/histogram.hpp>
#include <distwt/effective_alphabet.hpp>

struct wt_node_t {
    WaveletTree::bv_t bits;
    size_t            num;

    inline wt_node_t() : bits(), num(0) {
    }

    inline wt_node_t(WaveletTree::bv_t _bv, size_t _num)
        : bits(_bv), num(_num) {
    }

    inline operator bool() const {
        return bool(num);
    }
};

template<typename input_t>
void recursiveWT(
    wt_node_t* wt_nodes,
    const size_t node_id,
    input_t input,
    const size_t a,
    const size_t b) {

    if(a == b) {
        return;
    }/* else {
        input.Keep().Print(
            std::string("input_for_node_") + std::to_string(node_id));
    }*/

    const size_t m = (a + b) / 2;

    // compute node BV
    wt_nodes[node_id-1] = wt_node_t(input.Window(thrill::api::DisjointTag, 64,
        [m](size_t, const std::vector<esym_t>& v) {
            uint64_t bv = 0;
            for(size_t i = 0; i < v.size(); i++) {
                if(v[i] > m) {
                    bv |= (1ULL << (63ULL-i));
                }
            }
            return bv;
        })
        .Cache()
        .Execute(), input.Keep().Size());

    // "parallel split"

    // left child
    recursiveWT(
        wt_nodes,
        2ULL * node_id,
        input.Filter([m](const esym_t& c){ return (c <= m); }).Cache(),
        a, m);

    // right child
    recursiveWT(
        wt_nodes,
        2ULL * node_id + 1ULL,
        input.Filter([m](const esym_t& c){ return (c > m); }).Cache(),
        m+1, b);
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

    // allocate wt nodes
    const size_t sigma = hist.size();
    const size_t wt_height = tlx::integer_log2_ceil(sigma-1);
    const size_t wt_num_nodes = (1ULL << wt_height) - 1ULL;

    wt_node_t* wt_nodes = new wt_node_t[wt_num_nodes];
    for(size_t i = 0; i < wt_num_nodes; i++) {
        wt_nodes[i] = wt_node_t();
    }

    // construct recursively
    recursiveWT(
        wt_nodes,         // write into wt_nodes
        1ULL,             // start with root node
        etext,            // start with full transformed text
        0, wt_num_nodes); // start with [0, (2^h)-1] interval
                          // this is done to stay compatible with the other
                          // algorithms -> TODO: any negative effects?

    // store to disk
    for(size_t i = 0; i < wt_num_nodes; i++) {
        if(ctx.my_rank() == 0) {
            // write node length to file
            binary::FileWriter w(
                output + ".node_" + std::to_string(i+1) + ".len");
            w.write(wt_nodes[i].num);
        }

        // write bits
        if(wt_nodes[i]) {
            wt_nodes[i].bits.WriteBinary(
                output + ".node_" + std::to_string(i+1));
        }
    }

    // cleanup
    delete[] wt_nodes;

    if(ctx.my_rank() == 0) {
        // write histogram to file
        hist.save(output + ".hist");
    }

    // TODO: merge nodes on same level into consecutive bit vectors

    //WaveletTree wt(input_size, hist, recursiveWT(etext, 0, sigma-1));
    //wt.save(ctx, output);
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

