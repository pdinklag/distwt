#include <iostream>
#include <tuple>

#include <tlx/cmdline_parser.hpp>

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

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>

#include <distwt/thrill/wt_nodebased.hpp>
#include <distwt/thrill/wt_levelwise.hpp>

#include <thrill/common/stats_timer.hpp>
#include <distwt/thrill/result.hpp>

template<typename input_t>
void recursiveWT(
    WaveletTree::bits_t& bits,
    const WaveletTreeBase& wt,
    const size_t node_id,
    input_t input,
    const size_t a,
    const size_t b) {

    if(a == b) return;
    const size_t m = (a + b) / 2;

    // compute node BV
    bits[node_id - 1] = input.Window(thrill::api::DisjointTag, 64,
        [m](size_t, const std::vector<esym_t>& v) {
            uint64_t bv = 0;
            for(size_t i = 0; i < v.size(); i++) {
                if(v[i] > m) {
                    bv |= (1ULL << (63ULL-i));
                }
            }
            return bv;
        })
        .Cache();

    // left child
    auto input_l = input.Filter([m](const esym_t& c){ return (c <= m); });
    recursiveWT(bits, wt, 2ULL * node_id, input_l.Cache(), a, m);

    // right child
    auto input_r = input.Filter([m](const esym_t& c){ return (c >  m); });
    recursiveWT(bits, wt, 2ULL * node_id + 1ULL, input_r.Cache(), m+1, b);
}

void Process(
    thrill::Context& ctx,
    const std::string& input,
    const size_t input_size,
    const std::string& output) {

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, input).Cache();

    // compute histogram
    Histogram hist(rawtext);

    // compute effective alphabet
    EffectiveAlphabet ea(hist);

    // transform text
    auto etext = ea.transform(rawtext).Cache();

    // construct wt recursively
    WaveletTreeNodebased wt_nodes(hist,
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
        const size_t num_nodes = wt.num_nodes();
        bits.resize(num_nodes);

        recursiveWT(
            bits,
            wt,
            1ULL,             // start with root node
            etext,            // start with full transformed text
            0, num_nodes);
                              // start with [0, (2^h)-1] interval
                              // this is done to stay compatible with the other
                              // algorithms -> TODO: any negative side effects?
    });

    // Merge to levelwise wavelet tree
    auto wt = wt_nodes.merge(ctx, hist);

    if(output.length() > 0) {
        // store to disk
        hist.save(output + "." + WaveletTreeBase::histogram_extension());
        wt.save(output);
    } else {
        // make sure to actually compute the wavelet tree
        wt.ensure();
    }
}

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;

    std::string input_filename; // required
    std::string output_filename = "";

    cp.add_param_string("file", input_filename, "The input file.");
    cp.add_string('o', "out", output_filename, "The base output filename.");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        thrill::common::StatsTimer timer(true);

        const size_t input_size = util::file_size(input_filename);
        Process(
            ctx,
            input_filename,
            input_size,
            output_filename);

        // gather stats
        timer.Stop();
        Result result("thrill-dd", ctx, input_filename, input_size, timer.SecondsDouble());
        if(ctx.my_rank() == 0) {
            LOG1 << result.sqlplot();
            LOG1 << result.readable();
        }
    });
}
