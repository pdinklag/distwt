#pragma once

#include <string>
#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <distwt/common/histogram.hpp>

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
    static std::vector<size_t> node_sizes(const HistogramBase& hist);

    static inline std::string histogram_extension() {
        return "hist";
    }

    static inline std::string level_extension(size_t level) {
        return "lv_" + std::to_string(level+1);
    }

    static inline std::string node_extension(size_t node_id) {
        return "node_" + std::to_string(node_id);
    }

    WaveletTreeBase(const HistogramBase& hist);

    inline size_t sigma() const { return m_sigma; }
    inline size_t height() const { return m_height; }
    inline size_t num_nodes() const { return max_bintree_nodes(m_height); }
};
