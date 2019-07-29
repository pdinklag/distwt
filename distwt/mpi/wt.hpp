#pragma once

#include <distwt/common/wt.hpp>
#include <distwt/mpi/bit_vector.hpp>
#include <distwt/mpi/histogram.hpp>

#include <functional>

class WaveletTree : public WaveletTreeBase {
public:
    using bits_t = std::vector<bv_t>;

    using ctor_t = std::function<
        void(bits_t& bits, const WaveletTreeBase& wt)>;

protected:
    bits_t m_bits; // layout determined by implementation!

public:
    template<typename sym_t>
    inline WaveletTree(const Histogram<sym_t>& hist) : WaveletTreeBase(hist) {
    }

    template<typename sym_t>
    inline WaveletTree(const Histogram<sym_t>& hist, ctor_t construction_algorithm)
        : WaveletTreeBase(hist) {

        construction_algorithm(m_bits, *this);
    }
};
