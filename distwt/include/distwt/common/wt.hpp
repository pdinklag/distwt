#pragma once

#include <string>
#include <vector>

#include <distwt/common/histogram.hpp>

class WaveletTreeBase {
protected:
    size_t m_sigma;
    size_t m_height;

    std::vector<size_t> m_node_sizes;

public:
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
    inline size_t num_nodes() const { return (1ULL << m_height) - 1ULL; }

    inline const std::vector<size_t>& node_sizes() const {
        return m_node_sizes;
    }
};
