#include <iostream>
#include <utility>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/div_ceil.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <distwt/thrill/force.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/sym_pack.hpp>
#include <distwt/thrill/wm.hpp>

#include <thrill/common/stats_timer.hpp>
#include <distwt/thrill/result.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

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

        // construct wm
        WaveletMatrix wm(hist,
        [&](WaveletMatrix::bits_t& bits,
            WaveletMatrix::z_t& z,
            const WaveletMatrixBase& wm) {

            const size_t height = wm.height();

            bits.resize(height);

            auto text = etext.Collapse();
            for(size_t level = 0; level < height; level++) {
                const size_t rsh = height - 1 - level;

                // compute and store BV
                bits[level] = text.Keep().Window(thrill::api::DisjointTag, 64,
                    [rsh](size_t, const std::vector<sym_t>& v){
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

                // compute Z ... NOT doable during winow
                const size_t num1 = bits[level].Keep()
                    .Map([](const bv64_t& bv){return bv.count();})
                    .Sum([](const size_t a, const size_t b){return a + b;});
                const size_t num0 = input_size - num1;
                z[level] = num0;

                // stable bucket sort of text
                if(level+1 < height) {
                    // create buckets
                    auto bucket0 = text.Filter([rsh](sym_t x){
                        return (((x >> rsh) & 1) == 0);
                    });

                    auto bucket1 = text.Filter([rsh](sym_t x){
                        return (((x >> rsh) & 1) == 1);
                    });

                    // concatenate buckets
                    text = thrill::api::Concat(bucket0, bucket1);
                }
            }
        });

        wm.ensure();

        time.construct = timer.SecondsDouble();
        time.merge = 0; // no merging needed!

        // store to disk if needed
        if(output_filename.length() > 0) {
            if(ctx.my_rank() == 0) {
                hist.save(output_filename + "." +
                    WaveletMatrixBase::histogram_extension());
                wm.save_z(output_filename  + "." +
                    WaveletMatrixBase::z_extension());
            }

            wm.save(output_filename);
        }

        // gather stats
        timer.Stop();
        Result result("thrill-wm-concat",
            ctx, input_filename, input_size, hist.size(), time);

        if(ctx.my_rank() == 0) {
            LOG1 << result.readable();
            std::cout << result.sqlplot() << std::endl;
        }
    });
}
