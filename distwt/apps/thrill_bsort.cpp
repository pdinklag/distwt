#include <iostream>
#include <utility>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/div_ceil.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <distwt/thrill/force.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/esym_pack.hpp>
#include <distwt/thrill/wt_levelwise.hpp>

#include <thrill/common/stats_timer.hpp>
#include <distwt/thrill/result.hpp>

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

        // construct wt
        WaveletTreeLevelwise wt(hist,
        [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
            const size_t height = wt.height();
            const auto& node_sizes = wt.node_sizes();

            bits.resize(height);

            auto text = etext.Collapse();
            for(size_t level = 0; level < height; level++) {
                const size_t rsh = height - 1 - level;

                // compute and store BV
                bits[level] = text.Keep().Window(thrill::api::DisjointTag, 64,
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

                // stable bucket sort of text
                using indexed_pack_t = std::pair<size_t, esym_pack_word_t>;
                if(level+1 < height) {
                    const size_t num_nlevel_nodes = 1ULL << (level+1);
                    const size_t first_nlevel_node = num_nlevel_nodes;

                    // create buckets
                    std::vector<thrill::DIA<indexed_pack_t>> buckets(
                        num_nlevel_nodes);

                    // helper for discarding alignment bytes
                    std::vector<size_t> blocks(num_nlevel_nodes);

                    size_t glob_node_offs = 0;
                    for(size_t v = 0; v < num_nlevel_nodes; v++) {
                        buckets[v] =
                        (v+1 == num_nlevel_nodes ? text : text.Keep())
                        .Filter([rsh,v](esym_t x){ return ((x >> rsh) == v); })
                        .Window(thrill::api::DisjointTag, esym_pack_size,
                        [](size_t, const std::vector<esym_t>& v){
                            // pack symbols into 64-bit words
                            esym_pack_t pack;
                            for(size_t i = 0; i < v.size(); i++) {
                                pack.sym[i] = v[i];
                            }
                            return pack.packed;
                        })
                        .ZipWithIndex([glob_node_offs](
                            esym_pack_word_t x, size_t index) {

                            return indexed_pack_t(glob_node_offs+index, x);
                        });

                        // compute alignment data structure
                        const size_t node_id = first_nlevel_node + v;
                        const size_t num_blocks = tlx::div_ceil(
                            node_sizes[node_id-1], esym_pack_size);

                        blocks[v] = (v > 0)
                            ? (blocks[v-1] + num_blocks)
                            : num_blocks;

                        // advance
                        glob_node_offs += node_sizes[first_nlevel_node-1+v];
                    }

                    // concatenate buckets
                    text = thrill::api::Union(buckets)
                        .Sort(
                        [](const indexed_pack_t& a, const indexed_pack_t& b){
                            //sort by indices
                            return a.first < b.first;
                        })
                        .template FlatWindow<esym_t>(1,
                        [first_nlevel_node,node_sizes,blocks](
                        size_t rank,
                        const thrill::common::RingBuffer<indexed_pack_t>& v,
                        auto emit){
                            // unpack symbols from 64-bit word
                            esym_pack_t pack;
                            pack.packed = v[0].second;

                            // find node that current block belongs to
                            size_t i = 0;
                            while(blocks[i] <= rank) ++i;

                            // determine block size (<= esym_pack_size at node borders)
                            size_t block_size;
                            if(rank+1 == blocks[i]) {
                                const size_t sz_mod =
                                    node_sizes[first_nlevel_node+i-1] % esym_pack_size;

                                block_size = (sz_mod == 0ULL)
                                    ? esym_pack_size
                                    : sz_mod;
                            } else {
                                block_size = esym_pack_size;
                            }

                            // emit symbols, discard alignment bytes
                            for(size_t k = 0; k < block_size; k++) {
                                emit(pack.sym[k]);
                            }
                        })
                        .Collapse();
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
        Result result("thrill-bsort",
            ctx, input_filename, input_size, hist.size(), time);

        if(ctx.my_rank() == 0) {
            LOG1 << result.readable();
            std::cout << result.sqlplot() << std::endl;
        }
    });
}
