#pragma once

#include <cassert>
#include <distwt/common/effective_alphabet.hpp>
#include <distwt/common/wt.hpp>

// one bit vector per node
using wt_bits_t = std::vector<std::vector<bool>>;

// prefix counting for wavelet subtree
void wt_pc(
    wt_bits_t& bits,
    const std::vector<esym_t>& text,
    const size_t root_node,
    const size_t h);

// prefix counting
inline void wt_pc(
    const WaveletTreeBase& wt,
    wt_bits_t& bits,
    const std::vector<esym_t>& text) {

    wt_pc(bits, text, 1, wt.height());
}

// example algorithm from Navarro's book
void wt_navarro(
    wt_bits_t& bits,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer);

