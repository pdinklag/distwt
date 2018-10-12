#include <distwt/wt.hpp>

#include <thrill/api/read_binary.hpp>

void recursive_node_sizes(
    std::vector<size_t>& sizes,
    const std::vector<size_t>& c,
    size_t node_id,
    size_t a,
    size_t b) {

    if(a == b) return;

    sizes[node_id - 1] = c[std::min(b+1, c.size()-1)] - c[a];

    const size_t m = (a+b)/2;
    recursive_node_sizes(sizes, c, 2ULL * node_id, a, m);
    recursive_node_sizes(sizes, c, 2ULL * node_id + 1ULL, m+1, b);
}

std::vector<size_t> WaveletTree::node_sizes(const Histogram& hist) {
    const size_t n = num_nodes(hist);

    auto c = hist.compute_C();

    std::vector<size_t> sizes(n);
    recursive_node_sizes(sizes, c, 1, 0, n);

    return sizes;
}

void WaveletTree::save_histogram(const Histogram& hist) const {
    if(m_ctx->my_rank() == 0) {
        // write histogram to file
        hist.save(m_filename + ".hist");
    }
}

void WaveletTree::save_text_length(size_t len) const {
    if(m_ctx->my_rank() == 0) {
        // write text length to file
        binary::FileWriter w(m_filename + ".len");
        w.write(len);
    }
}

Histogram WaveletTree::load_histogram() {
    return Histogram(m_filename + ".hist");
}

size_t WaveletTree::load_text_length() {
    binary::FileReader r(m_filename + ".len");
    return r.template read<size_t>();
}

thrill::DIA<uint64_t> WaveletTree::load_level_bv(size_t level) {
    return thrill::api::ReadBinary<uint64_t>(
        *m_ctx, m_filename + "*.lv_" + std::to_string(level+1));
}
