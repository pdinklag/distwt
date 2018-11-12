#include <distwt/thrill/wt_nodebased.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/thrill/dia_prefix.hpp>

WaveletTreeNodebased::WaveletTreeNodebased(
    const Histogram& hist,
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
    thrill::Context& ctx, const Histogram& hist) {

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
