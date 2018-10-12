#pragma once

#include <distwt/binary_io.hpp>
#include <distwt/histogram.hpp>

#include <thrill/api/dia.hpp>
#include <thrill/api/write_binary.hpp>

class WaveletTree {
public:
    static inline size_t sigma(const Histogram& hist) {
        return hist.size();
    }

    static inline size_t height(const Histogram& hist) {
        return tlx::integer_log2_ceil(sigma(hist) - 1);
    }

private:
    thrill::Context* m_ctx; // current Thrill context
    std::string m_filename; // base filename

public:
    inline WaveletTree(thrill::Context& ctx, const std::string& filename)
        : m_ctx(&ctx), m_filename(filename) {
    }

    inline ~WaveletTree() {
    }

    void save_histogram(const Histogram& hist) const;
    void save_text_length(size_t len) const;

    template<typename bv_t>
    void save_level_bv(size_t level, bv_t&& bv) const {
        bv.WriteBinary(m_filename + ".lv_" + std::to_string(level+1));
    }

    template<typename bv_t>
    void save_node_bv(size_t node_id, bv_t&& bv, size_t len) const {
        auto ext = std::string(".node_") + std::to_string(node_id);

        // write bit vector
        bv.WriteBinary(m_filename + ext);

        if(m_ctx->my_rank() == 0) {
            // write node length
            binary::FileWriter w(m_filename + ext + ".len");
            w.write(len);
        }
    }

    Histogram             load_histogram();
    size_t                load_text_length();
    thrill::DIA<uint64_t> load_level_bv(size_t level);
};
