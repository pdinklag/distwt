#include <iostream>
#include <tuple>

#include <tlx/cmdline_parser.hpp>

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
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/wt_levelwise.hpp>

#include <thrill/common/stats_timer.hpp>
#include <distwt/thrill/result.hpp>

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
        auto rawtext = thrill::api::ReadBinary<rawsym_t>(
            ctx, input_filename, input_size).Cache().Execute();

        time.input = timer.SecondsDouble();
        timer.Reset();

        // compute histogram
        Histogram hist(rawtext);

        time.hist = timer.SecondsDouble();
        timer.Reset();

        // compute effective alphabet
        EffectiveAlphabet ea(hist);

        // transform text
        auto etext = ea.transform(rawtext).Cache().Execute();

        time.eff = timer.SecondsDouble();
        timer.Reset();

        // construct wt
        WaveletTreeLevelwise wt(hist,
        [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
            const size_t height = wt.height();
            bits.resize(height);

            auto text = etext;
            for(size_t level = 0; level < height; level++) {
                const size_t rsh = height - 1 - level;

                // compute and store BV
                bits[level] = text.Window(thrill::api::DisjointTag, 64,
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
                    .Cache();

                if(level+1 < height) {
                    text = text.SortStable(
                        [rsh](esym_t a, esym_t b){
                            // stably sort according to newest bit
                            return (a >> rsh) < (b >> rsh);
                        }).Collapse();
                }
            }
        });

        wt.ensure();

        time.construct = timer.SecondsDouble();
        time.merge = 0; // no merging needed!

        // store to disk if needed
        if(output_filename.length() > 0) {
            hist.save(output_filename + "." +
                WaveletTreeBase::histogram_extension());
            wt.save(output_filename);
        }

        // gather stats
        timer.Stop();
        Result result("thrill-sort",
            ctx, input_filename, input_size, hist.size(), time);

        if(ctx.my_rank() == 0) {
            LOG1 << result.readable();
            std::cout << result.sqlplot() << std::endl;
        }
    });
}
