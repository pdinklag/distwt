#pragma once

#include <distwt/common/wm.hpp>

#include <distwt/thrill/bv64.hpp>
#include <distwt/thrill/histogram.hpp>

#include <functional>
#include <thrill/api/dia.hpp>
#include <thrill/api/size.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

class WaveletMatrix : public WaveletMatrixBase {
public:
    using bv_t = thrill::DIA<bv64_t>;
    using bits_t = std::vector<bv_t>;
    using z_t    = std::vector<size_t>;

    using ctor_t = std::function<
        void(bits_t& bits, z_t& z, const WaveletMatrixBase& wm)>;

protected:
    bits_t m_bits; // layout determined by implementation!

public:
    inline WaveletMatrix(const Histogram& hist) : WaveletMatrixBase(hist) {
    }

    inline WaveletMatrix(const Histogram& hist, ctor_t construction_algorithm)
        : WaveletMatrixBase(hist) {

        construction_algorithm(m_bits, m_z, *this);
    }

    void save(const std::string& filename);

    inline void ensure() {
        for(auto& bv : m_bits) {
            // Force computation
            bv.Keep().Size();
        }
    }
};
