#include <distwt/wt.hpp>

#include <thrill/api/read_binary.hpp>

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
