#include <distwt/thrill/wt.hpp>

#include <thrill/api/read_binary.hpp>

void WaveletTree::save_histogram(const Histogram& hist) const {
    if(m_ctx->my_rank() == 0) {
        // write histogram to file
        hist.save(m_filename + ".hist");
    }
}

Histogram WaveletTree::load_histogram() {
    return Histogram(m_filename + ".hist");
}

thrill::DIA<uint64_t> WaveletTree::load_level_bv(size_t level) {
    return thrill::api::ReadBinary<uint64_t>(
        *m_ctx, m_filename + "*.lv_" + std::to_string(level+1));
}

thrill::DIA<uint64_t> WaveletTree::load_node_bv(size_t node_id) {
    return thrill::api::ReadBinary<uint64_t>(
        *m_ctx, m_filename + "*.node_" + std::to_string(node_id));
}
