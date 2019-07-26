#include <distwt/thrill/wt_nodebased.hpp>

#include <tlx/math/div_ceil.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <distwt/thrill/dia_prefix.hpp>

WaveletTreeNodebased::WaveletTreeNodebased(
    const HistogramBase<sym_t>& hist,
    thrill::Context& ctx,
    const std::string& filename) : WaveletTree(hist) {

    const size_t nodes = num_nodes();

    m_bits.resize(nodes);

    m_node_sizes = WaveletTreeBase::node_sizes(hist);
    for(size_t i = 0; i < nodes; i++) {
        if(m_node_sizes[i] > 0) {
            m_bits[i] = thrill::api::ReadBinary<bv64_t>(
                ctx, filename + "*." + node_extension(i+1));
        } else {
            m_bits[i] = thrill::api::Generate(
                ctx, 0, [](size_t){return bv64_t(0);});
        }
    }
}

void WaveletTreeNodebased::save(const std::string& filename) {
    for(size_t i = 0; i < num_nodes(); i++) {
        m_bits[i].WriteBinary(filename + "." + node_extension(i+1));
    }
}

thrill::DIA<sym_t> WaveletTreeNodebased::read_node(
    thrill::Context& ctx, size_t node_id, size_t level) {

    const size_t lsh = height() - 1ULL - level;

    return dia_prefix<sym_t>(m_bits[node_id-1]
        .template FlatMap<sym_t>([lsh](const bv64_t& x, auto emit) {
            // expand bits to 64 sym_t values
            for(size_t i = 0; i < 64; ++i) {
                emit(sym_t(x[63ULL-i]) << lsh);
            }
        }), m_node_sizes[node_id-1])
        .Collapse();
}

thrill::DIA<WaveletTreeNodebased::sym_index_t>
WaveletTreeNodebased::decode_recursive(
    thrill::DIA<WaveletTreeNodebased::sym_index_t> s,
    thrill::Context& ctx,
    size_t node_id,
    size_t level) {

    auto zipped = s.Zip(
        read_node(ctx, node_id, level),
        [](const sym_index_t& x, sym_t c){
            return sym_index_t(x.first | c, x.second);
        }).Cache();

    if(level + 1 < height()) {
        // recurse
        const size_t rsh = height() - 1ULL - level;

        auto l = decode_recursive(
            zipped.Filter([rsh](const sym_index_t& x){
                return !bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, 2ULL * node_id, level + 1);

        auto r = decode_recursive(
            zipped.Filter([rsh](const sym_index_t& x){
                return bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, 2ULL * node_id + 1ULL, level + 1);

        return l.Concat(r).Cache();
    } else {
        return zipped;
    }
}

rawtext_t WaveletTreeNodebased::decode(
    thrill::Context& ctx, const HistogramBase<sym_t>& hist) {

    // restore text length
    const size_t n = hist.text_length();

    // decode
    auto xtext = thrill::api::Generate(ctx, n,
        [](size_t i){ return sym_index_t(0, i); }
    );

    return decode_recursive(xtext, ctx, 1, 0)
        .Sort([](const sym_index_t& a, const sym_index_t& b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](const sym_index_t& x){
            // undo effective transformation
            return hist.entries[x.first].first;
        })
        .Collapse();
}

#include <distwt/thrill/wt_levelwise.hpp>

WaveletTreeLevelwise WaveletTreeNodebased::merge(
    thrill::Context& ctx, const HistogramBase<sym_t>& hist) {

    return WaveletTreeLevelwise(hist, // TODO: avoid recomputations!
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        bits.resize(height());
        bits[0] = m_bits[0].Cache();

        // for each level
        for(size_t level = 1; level < height(); level++) {
            const size_t num_level_nodes = 1ULL << level;
            const size_t first_level_node = num_level_nodes;

            // index 64-bit blocks of each node
            using indexed_block_t = std::pair<size_t, bv64_t>;

            std::vector<thrill::DIA<indexed_block_t>> indexed(num_level_nodes);
            std::vector<size_t> blocks(num_level_nodes);

            size_t level_node_offs = 0;
            for(size_t i = 0; i < num_level_nodes; i++) {
                const size_t node_id = first_level_node + i;

                indexed[i] = m_bits[node_id-1]
                    .ZipWithIndex([level_node_offs](bv64_t block, size_t index){
                        return indexed_block_t(level_node_offs+index, block);
                    });

                // compute alignment data structure
                const size_t sz = m_node_sizes[node_id-1];
                const size_t num_blocks = tlx::div_ceil(sz, 64ULL);

                blocks[i] = (i > 0) ? (blocks[i-1] + num_blocks) : num_blocks;

                // advance
                level_node_offs += num_blocks;
            }

            bits[level] =
                thrill::api::Union(indexed) // union indexed blocks to level
                .Sort([](const indexed_block_t& a, const indexed_block_t& b){
                    //sort by indices
                    return a.first < b.first;
                })
                .template FlatWindow<bool>(1,
                [this,first_level_node,blocks](
                    size_t rank,
                    const thrill::common::RingBuffer<indexed_block_t>& v,
                    auto emit){

                    // remove alignments
                    // TODO: is there any way to do this not via a bool DIA?

                    // find node that current block belongs to
                    size_t i = 0;
                    while(blocks[i] <= rank) ++i;

                    // determine block size (<= 64 at node borders)
                    size_t block_size;
                    if(rank+1 == blocks[i]) {
                        const size_t sz_mod64 =
                            m_node_sizes[first_level_node+i-1] % 64ULL;

                        block_size = (sz_mod64 == 0ULL) ? 64ULL : sz_mod64;
                    } else {
                        block_size = 64ULL;
                    }

                    // emit bits from block
                    bv64_t block_bits(v[0].second);
                    for(size_t k = 0; k < block_size; k++) {
                        emit(block_bits[63ULL-k]);
                    }
                })
                .Window(thrill::api::DisjointTag, 64,
                [](size_t, const std::vector<bool>& bv){

                    // pack bools back into bit vectors
                    bv64_t window_bits;
                    for(size_t i = 0; i < bv.size(); i++) {
                        window_bits[63ULL-i] = bv[i];
                    }
                    return window_bits;
                })
                .Cache();
        }
    });
}
