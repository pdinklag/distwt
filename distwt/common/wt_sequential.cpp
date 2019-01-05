#include <cassert>
#include <distwt/common/wt_sequential.hpp>

// prefix counting
void wt_pc(
    const WaveletTreeBase& wt,
    wt_bits_t& bits,
    const std::vector<esym_t>& text) {

    const size_t n = text.size();
    const size_t h = wt.height();
    const size_t sigma = 1ULL << h; // NOT wt.sigma()
                                    // we need the next power of two!

    assert(h > 1);

    // compute histogram and root node
    std::vector<size_t> hist(sigma);
    {
        const size_t rsh = h-1;
        auto& root = bits[0];
        root.resize(n);

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            ++hist[c];
            root[i] = (c >> rsh) & 1;
        }
    }

    // compute the rest bottom-up
    std::vector<size_t> count(sigma/2); // allocate counters
    for(size_t level = h-1; level > 0; --level) {
        const size_t num_level_nodes = (1ULL << level);
        const size_t first_level_node = num_level_nodes - 1;

        // compute new histogram, allocate nodes and reset counters
        for(size_t v = 0; v < num_level_nodes; v++) {
            const size_t size = hist[2 * v] + hist[2 * v + 1];
            const size_t node = first_level_node + v;

            hist[v] = size;
            bits[node].resize(size);
            count[v] = 0;
        }

        // compute level bit vectors
        const size_t rsh  = h - 1 - (level-1);
        const size_t test = 1ULL << (h - 1 - level);

        for(size_t i = 0; i < n; i++) {
            const size_t c = text[i];
            const size_t v = (c >> rsh);
            //assert(v < num_level_nodes);
            const size_t pos = count[v]++;
            const size_t node = first_level_node + v;
            const bool b = c & test;

            //assert(pos < hist[v]);
            bits[node][pos] = b;
        }
    }
}

// example algorithm from Navarro's book
void wt_navarro(
    wt_bits_t& bits,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer) {

    if(a == b) return;

    const size_t m = (a + b) / 2;

    // compute node bit vector
    size_t z = 0;
    {
        auto& bv = bits[node_id-1];
        bv.resize(n);

        for(size_t i = 0; i < n; i++) {
            if(text[i] <= m) {
                bv[i] = 0;
                ++z;
            } else {
                bv[i] = 1;
            }
        }
    }

    // only continue if needed
    if(a < m || m+1 < b) {
        // compute permutation of text in buffer
        {
            esym_t* pl = buffer;
            esym_t* pr = buffer + z;

            for(size_t i = 0; i < n; i++) {
                auto c = text[i];
                if(c <= m) {
                    *pl++ = c;
                } else {
                    *pr++ = c;
                }
            }
        }

        // recurse on left part
        wt_navarro(
            bits,
            2ULL * node_id, // left child
            buffer, z,
            a, m,
            text);

        // recurse on right part
        wt_navarro(
            bits,
            2ULL * node_id + 1ULL, // right child
            buffer + z, n - z,
            m+1, b,
            text + z);
    }
}

