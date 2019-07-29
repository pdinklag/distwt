#pragma once

#include <distwt/thrill/wt.hpp>

#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/text.hpp>

#include <thrill/api/context.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

class WaveletTreeLevelwise; // fwd
class WaveletTreeNodebased : public WaveletTree {
private:
    using sym_index_t = std::pair<sym_t, size_t>;

    std::vector<size_t> m_node_sizes;

    thrill::DIA<sym_t> read_node(
        thrill::Context& ctx, size_t node_id, size_t level);

    thrill::DIA<sym_index_t> decode_recursive(
        thrill::DIA<sym_index_t> s,
        thrill::Context& ctx,
        size_t node_id,
        size_t level);

public:
    inline WaveletTreeNodebased(
        const Histogram& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    WaveletTreeNodebased(
        const Histogram& hist,
        thrill::Context& ctx,
        const std::string& filename);

    void save(const std::string& filename);

    rawtext_t decode(thrill::Context& ctx, const Histogram& hist);

    WaveletTreeLevelwise merge(thrill::Context& ctx, const Histogram& hist);
};
