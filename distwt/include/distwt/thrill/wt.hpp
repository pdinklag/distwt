#pragma once

#include <distwt/common/binary_io.hpp>
#include <distwt/thrill/histogram.hpp>

#include <thrill/api/dia.hpp>
#include <thrill/api/write_binary.hpp>

class WaveletTree {
private:
    thrill::Context* m_ctx; // current Thrill context
    std::string m_filename; // base filename

public:
    static inline size_t sigma(const Histogram& hist) {
        return hist.size();
    }

    static inline size_t height(const Histogram& hist) {
        return tlx::integer_log2_ceil(sigma(hist) - 1);
    }

    static inline size_t num_nodes(const Histogram& hist) {
        return (1ULL << height(hist)) - 1ULL;
    }

    static std::vector<size_t> node_sizes(const Histogram& hist);

    inline WaveletTree(thrill::Context& ctx, const std::string& filename)
        : m_ctx(&ctx), m_filename(filename) {
    }

    inline ~WaveletTree() {
    }

    void save_histogram(const Histogram& hist) const;

    template<typename bv_t>
    void save_level_bv(size_t level, bv_t&& bv) const {
        bv.WriteBinary(m_filename + ".lv_" + std::to_string(level+1));
    }

    template<typename bv_t>
    void save_node_bv(size_t node_id, bv_t&& bv) const {
        bv.WriteBinary(m_filename + ".node_" + std::to_string(node_id));
    }

    Histogram             load_histogram();
    thrill::DIA<uint64_t> load_level_bv(size_t level);
    thrill::DIA<uint64_t> load_node_bv(size_t node_id);
};
