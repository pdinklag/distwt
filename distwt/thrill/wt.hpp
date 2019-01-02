#pragma once

#include <distwt/common/wt.hpp>
#include <distwt/common/histogram.hpp>
#include <distwt/thrill/bv64.hpp>

#include <functional>
#include <thrill/api/dia.hpp>
#include <thrill/api/size.hpp>

class WaveletTree : public WaveletTreeBase {
public:
    using bv_t = thrill::DIA<bv64_t>;
    using bits_t = std::vector<bv_t>;

    using ctor_t = std::function<
        void(bits_t& bits, const WaveletTreeBase& wt)>;

protected:
    bits_t m_bits; // layout determined by implementation!

public:
    inline WaveletTree(const HistogramBase& hist) : WaveletTreeBase(hist) {
    }

    inline WaveletTree(const HistogramBase& hist, ctor_t construction_algorithm)
        : WaveletTreeBase(hist) {

        construction_algorithm(m_bits, *this);
    }

    inline void ensure() {
        for(auto& bv : m_bits) {
            // Force computation
            bv.Size();
        }
    }
};
