#include <distwt/thrill/wt_nodebased.hpp>

#include <tlx/math/div_ceil.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

#include <distwt/thrill/dia_prefix.hpp>

// DEBUG
#include <distwt/thrill/bitset.hpp>
#include <thrill/api/print.hpp>

WaveletTreeNodebased::WaveletTreeNodebased(
    const HistogramBase& hist,
    thrill::Context& ctx,
    const std::string& filename) : WaveletTree(hist) {

    const size_t nodes = num_nodes();

    m_bits.resize(nodes);
    for(size_t i = 0; i < nodes; i++) {
        if(m_node_sizes[i] > 0) {
            m_bits[i] = thrill::api::ReadBinary<uint64_t>(
                ctx, filename + "*." + node_extension(i+1));
        } else {
            m_bits[i] = thrill::api::Generate(
                ctx, 0, [](size_t){return uint64_t(0);});
        }
    }
}

void WaveletTreeNodebased::save(const std::string& filename) {
    for(size_t i = 0; i < num_nodes(); i++) {
        m_bits[i].WriteBinary(filename + "." + node_extension(i+1));
    }
}

thrill::DIA<esym_t> WaveletTreeNodebased::read_node(
    thrill::Context& ctx, size_t node_id, size_t level) {

    const size_t lsh = height() - 1ULL - level;

    return dia_prefix<esym_t>(m_bits[node_id-1]
        .template FlatMap<esym_t>([lsh](const uint64_t& x, auto emit) {
            // expand bits to 64 esym_t values
            for(size_t i = 0; i < 64; ++i) {
                bool b = (x & (1ULL << (63ULL - i)));
                emit(esym_t(b) << lsh);
            }
        }), m_node_sizes[node_id-1])
        .Collapse();
}

thrill::DIA<WaveletTreeNodebased::esym_index_t>
WaveletTreeNodebased::decode_recursive(
    thrill::DIA<WaveletTreeNodebased::esym_index_t> s,
    thrill::Context& ctx,
    size_t node_id,
    size_t level) {

    auto zipped = s.Zip(
        read_node(ctx, node_id, level),
        [](const esym_index_t& x, esym_t c){
            return esym_index_t(x.first | c, x.second);
        }).Cache();

    if(level + 1 < height()) {
        // recurse
        const size_t rsh = height() - 1ULL - level;

        auto l = decode_recursive(
            zipped.Filter([rsh](const esym_index_t& x){
                return !bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, 2ULL * node_id, level + 1);

        auto r = decode_recursive(
            zipped.Filter([rsh](const esym_index_t& x){
                return bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, 2ULL * node_id + 1ULL, level + 1);

        return l.Concat(r).Cache();
    } else {
        return zipped;
    }
}

rawtext_t WaveletTreeNodebased::decode(
    thrill::Context& ctx, const HistogramBase& hist) {

    // restore text length
    const size_t n = hist.text_length();

    // decode
    auto xtext = thrill::api::Generate(ctx, n,
        [](size_t i){ return esym_index_t(0, i); }
    );

    return decode_recursive(xtext, ctx, 1, 0)
        .Sort([](const esym_index_t& a, const esym_index_t& b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](const esym_index_t& x){
            // undo effective transformation
            return hist.entries[x.first].first;
        })
        .Collapse();
}

#include <distwt/thrill/wt_levelwise.hpp>

WaveletTreeLevelwise WaveletTreeNodebased::merge(
    thrill::Context& ctx, const HistogramBase& hist) {

    return WaveletTreeLevelwise(hist, // TODO: avoid recomputations!
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        bits.resize(height());
        bits[0] = m_bits[0]; // simply copy root

        // for each level
        for(size_t level = 1; level < height(); level++) {
            const size_t num_level_nodes = 1ULL << level;
            const size_t first_level_node = num_level_nodes;

            // index 64-bit blocks of each node
            using indexed_block_t = std::pair<size_t, uint64_t>;

            std::vector<thrill::DIA<indexed_block_t>> indexed(num_level_nodes);

            size_t level_node_offs = 0;
            for(size_t i = 0; i < num_level_nodes; i++) {
                const size_t node_id = first_level_node + i;

                indexed[i] = m_bits[node_id-1]
                    .ZipWithIndex([level_node_offs](uint64_t block, size_t index){
                        return indexed_block_t(level_node_offs+index, block);
                    });

                // compute alignment data structure
                const size_t sz = m_node_sizes[node_id-1];
                const size_t num_blocks = tlx::div_ceil(sz, 64ULL);

                // advance
                level_node_offs += num_blocks;
            }

            // union indexed blocks to level and sort by indices

            // DEBUG
            auto x = thrill::api::Union(indexed)
                .Sort([](const indexed_block_t& a, const indexed_block_t& b){
                    return a.first < b.first;
                });
            x.Print("x");

            bits[level] = x.template FlatWindow<uint64_t>(2,
                [&](
                    size_t rank,
                    const thrill::common::RingBuffer<indexed_block_t>& v,
                    auto emit){

                    // TODO: ???
                    emit(v[0].second);
                },
                [&](
                    size_t rank,
                    const thrill::common::RingBuffer<indexed_block_t>& v,
                    auto emit){

                    // TODO: ???
                    emit(v[0].second);
                });

            bits[level].Map([](uint64_t x){return bv64_t(x);}).Print("bv");
        }
    });

    // for each level l:
    //   - for each node on level l:
    //     - zip 64-bit blocks with indices (ZipWithIndex + node offset)
    //   - union indexed blocks (Union) to level
    //   - sort blocks by indices (Sort)
    //   - process indexed blocks using a 2-block window (Window), removing
    //     (a) the indices
    //     (b) any leftover alignment bits
}

inline std::ostream& operator<<(
    std::ostream& os, const std::pair<uint64_t, size_t>& x) {

    bv64_t bits = x.second;
    os << '[' << x.first << ": " << bits << ']';
    return os;
}
