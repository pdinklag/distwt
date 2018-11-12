#pragma once

#include <distwt/common/wt.hpp>
#include <distwt/thrill/histogram.hpp>

#include <functional>
#include <thrill/api/dia.hpp>

class WaveletTree : public WaveletTreeBase {
public:
    using bv_t = thrill::DIA<uint64_t>;
    using bits_t = std::vector<bv_t>;

    using ctor_t = std::function<
        void(bits_t& bits, const WaveletTreeBase& wt)>;

protected:
    bits_t m_bits; // layout determined by implementation!

public:
    inline WaveletTree(const Histogram& hist) : WaveletTreeBase(hist) {
    }

    inline WaveletTree(const Histogram& hist, ctor_t construction_algorithm)
        : WaveletTreeBase(hist) {

        construction_algorithm(m_bits, *this);
    }
};
