#pragma once

#include <distwt/common/wm.hpp>
#include <distwt/common/histogram.hpp>
#include <distwt/mpi/bit_vector.hpp>
#include <distwt/mpi/context.hpp>

#include <functional>

class WaveletMatrix : public WaveletMatrixBase {
public:
    using bits_t = std::vector<bv_t>;
    using z_t    = std::vector<size_t>;

    using ctor_t = std::function<
        void(bits_t& bits, z_t&, const WaveletMatrixBase& wt)>;

protected:
    bits_t m_bits; // one bit vector per level

public:
    template<typename sym_t>
    inline WaveletMatrix(const HistogramBase<sym_t>& hist) : WaveletMatrixBase(hist) {
    }

    template<typename sym_t>
    inline WaveletMatrix(const HistogramBase<sym_t>& hist, ctor_t construction_algorithm)
        : WaveletMatrixBase(hist) {

        construction_algorithm(m_bits, m_z, *this);
    }

    void save(const MPIContext& ctx, const std::string& output);
};
