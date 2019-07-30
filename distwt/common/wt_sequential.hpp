#pragma once

#include <distwt/common/effective_alphabet.hpp>
#include <distwt/common/wt.hpp>

#include <cassert>
#include <tlx/math/integer_log2.hpp>

// one bit vector per node
using wt_bits_t = std::vector<std::vector<bool>>;

// prefix counting for wavelet subtree
template<typename sym_t, typename idx_t>
inline void wt_pc(
    wt_bits_t& bits,
    const std::vector<sym_t>& text,
    const size_t root_node_id, // 1-based!!
    const size_t h) {

    assert(root_node_id > 0);
    const size_t root_level = tlx::integer_log2_floor(root_node_id);
    const size_t root_rank = (root_node_id - (1ULL << root_level));
    const size_t glob_h = root_level + h;

    const size_t n = text.size();
    const size_t sigma = 1ULL << h; // we need the next power of two!

    assert(h >= 1);

    // compute histogram and root node
    std::vector<idx_t> hist(sigma);
    {
        const size_t test = 1ULL << (glob_h - 1 - root_level);

        auto& root = bits[root_node_id-1];
        root.resize(n);

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const size_t glob_v = c;
            const size_t v = glob_v - root_rank * (1ULL << (glob_h-root_level));

            //assert(v < sigma);
            const bool b = c & test;

            ++hist[v];
            root[i] = b;
        }
    }

    // compute the rest bottom-up
    std::vector<idx_t> count(sigma/2); // allocate counters
    for(size_t level = h-1; level > 0; --level) {
        const size_t num_level_nodes = (1ULL << level);
        //const size_t first_level_node = num_level_nodes - 1;

        const size_t glob_level = root_level + level;
        const size_t glob_offs = ((1ULL << level) * root_node_id) - 1;

        // compute new histogram, allocate nodes and reset counters
        for(size_t v = 0; v < num_level_nodes; v++) {
            const size_t size = hist[2 * v] + hist[2 * v + 1];
            const size_t node = glob_offs + v;

            hist[v] = size;
            bits[node].resize(size);
            count[v] = 0;
        }

        // compute level bit vectors
        const size_t rsh  = glob_h - 1 - (glob_level-1);
        const size_t test = 1ULL << (glob_h - 1 - glob_level);

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const size_t glob_v = (c >> rsh);
            const size_t v = glob_v - root_rank * (1ULL << level);

            //assert(glob_v >= root_rank * (1ULL << level));
            //assert(v < num_level_nodes);
            const size_t node = glob_offs + v;

            const size_t pos = count[v];
            ++count[v];
            const bool b = c & test;

            //assert(pos < hist[v]);
            bits[node][pos] = b;
        }
    }
}

// prefix counting
template<typename sym_t, typename idx_t>
inline void wt_pc(
    const WaveletTreeBase& wt,
    wt_bits_t& bits,
    const std::vector<sym_t>& text) {

    wt_pc<sym_t, idx_t>(bits, text, 1, wt.height());
}

