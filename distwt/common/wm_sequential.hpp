#pragma once

#include <distwt/common/effective_alphabet.hpp>
#include <distwt/common/wm.hpp>

// one bit vector and one border per level
using wm_bits_t = std::vector<std::vector<bool>>;
using wm_z_t    = std::vector<size_t>;

// prefix counting for wavelet matrix
void wm_pc(
    const WaveletMatrixBase& wm,
    wm_bits_t& bits,
    wm_z_t& z,
    const std::vector<esym_t>& text);
