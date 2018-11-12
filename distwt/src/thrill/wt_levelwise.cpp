#include <distwt/thrill/wt_levelwise.hpp>

#include <tuple>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/dia_prefix.hpp>

WaveletTreeLevelwise::WaveletTreeLevelwise(
    const Histogram& hist,
    thrill::Context& ctx,
    const std::string& filename) : WaveletTree(hist) {

    m_bits.resize(height());
    for(size_t l = 0; l < height(); l++) {
        m_bits[l] = thrill::api::ReadBinary<uint64_t>(
            ctx, filename + "*." + level_extension(l));
    }
}

void WaveletTreeLevelwise::save(const std::string& filename) {
    for(size_t l = 0; l < height(); l++) {
        m_bits[l].WriteBinary(filename + "." + level_extension(l));
    }
}

rawtext_t WaveletTreeLevelwise::decode(
    thrill::Context& ctx, const Histogram& hist) {

    // indexed symbol
    using esym_index_t = std::pair<esym_t, size_t>;

    // restore text length
    const size_t n = hist.text_length();

    // construct indexed sequence of 0 symbols
    auto xtext = thrill::api::Generate(ctx, n, [](size_t i){
        return esym_index_t(0, i);}
    );

    // reconstruct symbols from bit vector level by level
    for(size_t level = 0; level < height(); level++) {
        const size_t lsh = height() - 1ULL - level;

        xtext = dia_prefix<esym_t>(m_bits[level]
            .template FlatMap<esym_t>([lsh](const uint64_t& x, auto emit) {
                // expand bits to 64 boolean values
                for(size_t i = 0; i < 64; ++i) {
                    bool b = (x & (1ULL << (63ULL - i)));
                    emit(esym_t(b) << lsh);
                }
            }), n)
            .Zip(xtext, [lsh](esym_t c, esym_index_t x) {
                // OR symbols using current vector
                return esym_index_t(x.first | c, x.second);
            })
            .SortStable([lsh](esym_index_t a, esym_index_t b){
                // stably reorder according to newest bit
                return (a.first >> lsh) < (b.first >> lsh);
            })
            .Collapse();
    }

    // restore original text
    return xtext
        .Sort([](esym_index_t a, esym_index_t b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](esym_index_t x){ // TODO: capture hist by reference?
            // undo effective transformation
            return hist.entries[x.first].first;
        })
        .Cache();
}
