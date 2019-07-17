#include <cassert>
#include <distwt/common/bitrev.hpp>
#include <distwt/common/wm_sequential.hpp>
#include <tlx/math/integer_log2.hpp>

void wm_pc(
    const WaveletMatrixBase& wm,
    wm_bits_t& bits,
    wm_z_t& z,
    const std::vector<esym_t>& text) {

    const size_t root_node_id = 1;
    const size_t h = wm.height();
    const size_t n = text.size();
    const size_t sigma = 1ULL << h; // we need the next power of two!

    assert(h > 1);

    // compute histogram and top level
    std::vector<size_t> hist(sigma);
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
    std::vector<size_t> borders(sigma/2); // allocate borders
    borders[0] = 0;
    
    for(size_t level = h-1; level > 0; --level) {
        // allocate bit vector
        bits[level].resize(n);
        
        const size_t num_level_nodes = (1ULL << level);
        const size_t glob_offs = (1ULL << level) - 1;

        // compute new histogram
        for(size_t v = 0; v < num_level_nodes; v++) {
            hist[v] = hist[2 * v] + hist[2 * v + 1];
        }

        // compute new borders
        {
            uint32_t prev = 0, vrev;
            for(size_t v = 1; v < num_level_nodes; v++) {
                vrev = bitrev(uint32_t(v), level + 1);
                borders[vrev] = borders[prev] + hist[prev];
                prev = vrev;
            }
        }

        // compute level bit vectors
        const size_t rsh  = h - 1 - (level - 1);
        const size_t test = 1ULL << (h - 1 - level);
        size_t num0 = 0;

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const size_t v = (c >> rsh);

            const size_t pos = borders[v]++;
            const bool b = c & test;

            if(!b) ++num0;
            bits[level][pos] = b;
        }
        z[level] = num0;
    }
}
