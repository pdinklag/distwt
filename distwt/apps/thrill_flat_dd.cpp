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

template<typename input_t>
void flatWT(
    WaveletTree::bits_t& bits,
    const WaveletTreeBase& wt,
    const Histogram& hist,
    input_t input) {

    // proceed level by level
    size_t node_id = 1ULL;
    for(size_t level = 0; level < wt.height(); level++) {
        const size_t rsh = wt.height() - 1ULL - level;
        const size_t pref = rsh + 1ULL;

        const size_t level_nodes = (1ULL << level);
        for(size_t i = 0; i < level_nodes; i++) {
            // for a symbol to go in this node, its bit prefix of length pref
            // has to be precisely i
            bits[(node_id++)-1] = input
                .Filter([pref, i](const esym_t& c){ return (c >> pref) == i; })
                .Window(thrill::api::DisjointTag, 64,
                [rsh](size_t, const std::vector<esym_t>& v) {
                    bv64_t bv;
                    for(size_t i = 0; i < v.size(); i++) {
                        // check level-th bit of symbol
                        if((v[i] >> rsh) & 1) {
                            bv[63ULL-i] = 1;
                        }
                    }
                    return bv;
                })
                .Cache();
        }
    }
}

void Process(
    thrill::Context& ctx,
    std::string input,
    size_t input_size,
    std::string output) {


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
        Histogram hist(ctx, rawtext);

        time.hist = timer.SecondsDouble();
        timer.Reset();

        if(ctx.my_rank() == 0) LOG1 << "Histogram computed!";

        // compute effective alphabet
        EffectiveAlphabet ea(hist);

        // transform text
        auto etext = thrill::api::ext::Force(ea.transform(rawtext));
        time.eff = timer.SecondsDouble();
        timer.Reset();

        // construct node-based wt
        WaveletTreeNodebased wt_nodes(hist,
        [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
            bits.resize(wt.num_nodes());
            flatWT(bits, wt, hist, etext);
        });

        wt_nodes.ensure(); // make sure to actually compute the wavelet tree
        time.construct = timer.SecondsDouble();
        timer.Reset();

        // Merge to levelwise wavelet tree
        auto wt = wt_nodes.merge(ctx, hist);
        wt.ensure(); // make sure to actually compute the wavelet tree

        time.merge = timer.SecondsDouble();

        // store to disk if needed
        if(output_filename.length() > 0) {
            hist.save(output_filename + "." +
                WaveletTreeBase::histogram_extension());
            wt.save(output_filename);
        }

        // gather stats
        Result result("thrill-flat-dd",
            ctx, input_filename, input_size, hist.size(), time);

        if(ctx.my_rank() == 0) {
            LOG1 << result.readable();
            std::cout << result.sqlplot() << std::endl;
        }
    });
}

