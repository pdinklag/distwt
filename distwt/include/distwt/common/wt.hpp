#pragma once

#include <tlx/math/integer_log2.hpp>
#include <distwt/common/histogram.hpp>

class WaveletTreeBase {
protected:
    std::string m_filename; // base filename

public:
    static inline size_t sigma(const HistogramBase& hist) {
        return hist.size();
    }

    static inline size_t height(const HistogramBase& hist) {
        return tlx::integer_log2_ceil(sigma(hist) - 1);
    }

    static inline size_t num_nodes(const HistogramBase& hist) {
        return (1ULL << height(hist)) - 1ULL;
    }

    static std::vector<size_t> node_sizes(const HistogramBase& hist);

    inline WaveletTreeBase(const std::string& filename)
        : m_filename(filename) {
    }
};
