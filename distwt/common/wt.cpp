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
