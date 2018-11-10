#pragma once

#include <distwt/common/wt.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/thrill/histogram.hpp>

#include <thrill/api/dia.hpp>
#include <thrill/api/write_binary.hpp>

class WaveletTree : public WaveletTreeBase {
private:
    thrill::Context* m_ctx; // current Thrill context

public:
    inline WaveletTree(thrill::Context& ctx, const std::string& filename)
        : WaveletTreeBase(filename), m_ctx(&ctx) {
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
