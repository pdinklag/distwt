#pragma once

#include <distwt/common/effective_alphabet.hpp>
#include <distwt/common/wm.hpp>

#include <cassert>
#include <distwt/common/bitrev.hpp>
#include <tlx/math/integer_log2.hpp>

// one bit vector and one border per level
using wm_bits_t = std::vector<std::vector<bool>>;
using wm_z_t    = std::vector<size_t>;

// prefix counting for wavelet matrix
template<typename sym_t, typename idx_t>
void wm_pc(
    const WaveletMatrixBase& wm,
    wm_bits_t& bits,
    wm_z_t& z,
    const std::vector<sym_t>& text) {

    const size_t root_node_id = 1;
    const size_t h = wm.height();
    const size_t n = text.size();
    const size_t sigma = 1ULL << h; // we need the next power of two!

    assert(h > 1);

    // compute histogram and top level
    std::vector<idx_t> hist(sigma);
    {
        const size_t test = 1ULL << (h - 1); // MSB
        size_t num0 = 0;

        auto& root = bits[0];
        root.resize(n);

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const bool b = c & test;

            ++hist[c];
            root[i] = b;
            if(!b) ++num0;
        }

        z[0] = num0;
    }

    // compute the rest bottom-up
    std::vector<idx_t> borders(sigma/2); // allocate borders

    for(size_t level = h-1; level > 0; --level) {
        // allocate bit vector
        bits[level].resize(n);

        const size_t num_level_nodes = (1ULL << level);

        // compute new histogram
        for(size_t v = 0; v < num_level_nodes; v++) {
            hist[v] = hist[2 * v] + hist[2 * v + 1];
        }

        // compute new borders
        {
            borders[0] = 0;
            uint32_t prev = 0, vrev;
            for(size_t v = 1; v < num_level_nodes; v++) {
                vrev = bitrev(uint32_t(v), level);
                borders[vrev] = borders[prev] + hist[prev];
                prev = vrev;
            }
        }

        // compute level bit vector
        auto& bv = bits[level];
        const size_t rsh  = h - 1 - (level - 1);
        const size_t test = 1ULL << (h - 1 - level);
        size_t num0 = 0;

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const size_t v = (c >> rsh);

            const size_t pos = borders[v];
            ++borders[v];
            const bool b = c & test;

            if(!b) ++num0;
            bv[pos] = b;
        }
        z[level] = num0;
    }
}
