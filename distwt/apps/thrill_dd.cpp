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
#include <distwt/thrill/force.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>
#include <distwt/thrill/text.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>

#include <distwt/thrill/wt_nodebased.hpp>
#include <distwt/thrill/wt_levelwise.hpp>

#include <thrill/common/stats_timer.hpp>
#include <distwt/thrill/result.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

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
    bits[node_id - 1] = input.Keep().Window(thrill::api::DisjointTag, 64,
        [m](size_t, const std::vector<sym_t>& v) {
            bv64_t bv;
            for(size_t i = 0; i < v.size(); i++) {
                if(v[i] > m) {
                    bv[63ULL-i] = 1;
                }
            }
            return bv;
        });

    // left child
    auto input_l = input.Keep().Filter([m](const sym_t& c){ return (c <= m); });
    recursiveWT(bits, wt, 2ULL * node_id, input_l.Collapse(), a, m);

    // right child
    auto input_r = input.Filter([m](const sym_t& c){ return (c >  m); });
    recursiveWT(bits, wt, 2ULL * node_id + 1ULL, input_r.Collapse(), m+1, b);
}

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;

    std::string input_filename; // required
    std::string output_filename = "";

    cp.add_param_string("file", input_filename, "The input file.");
    cp.add_string('o', "out", output_filename, "The base output filename.");

    size_t prefix = SIZE_MAX; // default to whole file
    cp.add_bytes('p', "prefix", prefix, "Only process prefix of input file.");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        ctx.enable_consume(true);

        Result::Time time;
        thrill::common::StatsTimer timer(true);

        const size_t input_size = std::min(
            util::file_size(input_filename), prefix);

        // load raw text
        auto rawtext = thrill::api::ext::Force(
            thrill::api::ReadBinary<rawsym_t>(ctx, input_filename, input_size)
            .Cache()
        ).Collapse();

        time.input = timer.SecondsDouble();
        timer.Reset();

        // compute histogram
        Histogram hist(ctx, rawtext.Keep());

        time.hist = timer.SecondsDouble();
        timer.Reset();

        if(ctx.my_rank() == 0) LOG1 << "Histogram computed!";

        // compute effective alphabet
        EffectiveAlphabet ea(hist);

        // transform text
        auto etext = thrill::api::ext::Force(ea.transform(rawtext));
        time.eff = timer.SecondsDouble();
        timer.Reset();

        if(ctx.my_rank() == 0) LOG1 << "Constructing WT ...";

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
        wt.ensure(); // make sure to actually compute the wavelet tree

        time.construct = timer.SecondsDouble();
        time.merge = 0; // we don't measure this separately

        // store to disk if needed
        if(output_filename.length() > 0) {
            hist.save(output_filename + "." +
                WaveletTreeBase::histogram_extension());
            wt.save(output_filename);
        }

        // gather stats
        Result result("thrill-dd",
            ctx, input_filename, input_size, hist.size(), time);

        if(ctx.my_rank() == 0) {
            LOG1 << result.readable();
            std::cout << result.sqlplot() << std::endl;
        }
    });
}
