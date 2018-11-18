#pragma once

#include <distwt/thrill/wt.hpp>

#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/text.hpp>

#include <thrill/api/context.hpp>

class WaveletTreeLevelwise; // fwd
class WaveletTreeNodebased : public WaveletTree {
private:
    using esym_index_t = std::pair<esym_t, size_t>;

    thrill::DIA<esym_t> read_node(
        thrill::Context& ctx, size_t node_id, size_t level);

    thrill::DIA<esym_index_t> decode_recursive(
        thrill::DIA<esym_index_t> s,
        thrill::Context& ctx,
        size_t node_id,
        size_t level);

public:
    inline WaveletTreeNodebased(
        const HistogramBase& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    WaveletTreeNodebased(
        const HistogramBase& hist,
        thrill::Context& ctx,
        const std::string& filename);

    void save(const std::string& filename);

    rawtext_t decode(thrill::Context& ctx, const HistogramBase& hist);

    WaveletTreeLevelwise merge(thrill::Context& ctx, const HistogramBase& hist);
};
