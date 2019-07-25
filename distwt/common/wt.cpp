#include <distwt/common/wt.hpp>

#include <algorithm>

void recursive_node_sizes(
    std::vector<size_t>& sizes,
    const std::vector<size_t>& c,
    size_t node_id,
    size_t a,
    size_t b) {

    if(a < b) {
        sizes[node_id - 1] =
            c[std::min(b+1, c.size()-1)] -
            c[std::min(a,   c.size()-1)];

        const size_t m = (a+b)/2;
        recursive_node_sizes(sizes, c, 2ULL * node_id, a, m);
        recursive_node_sizes(sizes, c, 2ULL * node_id + 1ULL, m+1, b);
    }
}

std::vector<size_t> WaveletTreeBase::node_sizes(const HistogramBase& hist) {
    const size_t num_nodes = max_bintree_nodes(wt_height(hist.size()));
    std::vector<size_t> sizes(num_nodes);
    
    auto c = hist.compute_C();
    recursive_node_sizes(sizes, c, 1, 0, num_nodes);

    return sizes;
}

WaveletTreeBase::WaveletTreeBase(const HistogramBase& hist)
    : m_sigma(hist.size()),
      m_height(wt_height(hist.size())) {
}
