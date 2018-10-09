#pragma once

#include <distwt/histogram.hpp>
#include <thrill/api/dia.hpp>

// DIA type used for distributed bit vectors
using bv_dia_t = thrill::DIA<uint64_t>;

// storage for a wavelet tree's bit vector
using wt_bits_t = std::vector<bv_dia_t>;

class WaveletTree {
public:
    using bv_t = thrill::DIA<uint64_t>;

private:
    size_t m_len;
    Histogram m_hist;
    std::vector<bv_t> m_bits;
    // TODO: pointers

public:
    inline WaveletTree(size_t len, Histogram hist, std::vector<bv_t> bits)
        : m_len(len), m_hist(hist), m_bits(bits) {
    }

    WaveletTree(thrill::Context& ctx, const std::string& filename);
    ~WaveletTree();

    void save(thrill::Context& ctx, const std::string& filename) const;

    inline const bv_t& bits(size_t level) const {
        return m_bits[level];
    }

    inline const Histogram& histogram() const {
        return m_hist;
    }

    inline size_t text_length() const {
        return m_len;
    }

    inline size_t height() const {
        return m_bits.size();
    }
};
