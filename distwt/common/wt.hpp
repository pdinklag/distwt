#pragma once

#include <string>
#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <distwt/common/histogram.hpp>

void recursive_node_sizes(
    std::vector<size_t>& sizes,
    const std::vector<size_t>& c,
    size_t node_id,
    size_t a,
    size_t b);

class WaveletTreeBase {
protected:
    size_t m_sigma;
    size_t m_height;

    static inline size_t wt_height(const size_t sigma) {
        return tlx::integer_log2_ceil(sigma - 1);
    }

    static inline size_t max_bintree_nodes(const size_t height) {
        return (1ULL << height) - 1ULL;
    }

public:
    template<typename sym_t>
    static std::vector<size_t> node_sizes(const HistogramBase<sym_t>& hist) {
        const size_t num_nodes = max_bintree_nodes(wt_height(hist.size()));
        std::vector<size_t> sizes(num_nodes);

        auto c = hist.compute_C();
        recursive_node_sizes(sizes, c, 1, 0, num_nodes);

        return sizes;
    }

    static inline std::string histogram_extension() {
        return "hist";
    }

    static inline std::string level_extension(size_t level) {
        return "lv_" + std::to_string(level+1);
    }

    static inline std::string node_extension(size_t node_id) {
        return "node_" + std::to_string(node_id);
    }

    template<typename sym_t>
    inline WaveletTreeBase(const HistogramBase<sym_t>& hist)
        : m_sigma(hist.size()),
          m_height(wt_height(hist.size())) {
    }

    inline size_t sigma() const { return m_sigma; }
    inline size_t height() const { return m_height; }
    inline size_t num_nodes() const { return max_bintree_nodes(m_height); }
};
