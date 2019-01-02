#pragma once

#include <cassert>
#include <distwt/common/effective_alphabet.hpp>
#include <distwt/common/wt.hpp>

// one bit vector per node
using wt_bits_t = std::vector<std::vector<bool>>;

// prefix counting
void wt_pc(
    const WaveletTreeBase& wt,
    wt_bits_t& bits,
    const std::vector<esym_t>& text);

// example algorithm from Navarro's book
void wt_navarro(
    wt_bits_t& bits,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer);

