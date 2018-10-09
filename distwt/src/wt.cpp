#include <distwt/wt.hpp>
#include <distwt/binary_io.hpp>

#include <thrill/api/read_binary.hpp>
#include <thrill/api/write_binary.hpp>

WaveletTree::WaveletTree(
    thrill::Context& ctx,
    const std::string& filename)
    : m_hist(filename + ".hist") {

    // read text length
    {
        binary::FileReader r(filename + ".len");
        m_len = r.template read<size_t>();
    }

    // compute height and read bits
    const size_t sigma = m_hist.size();
    const size_t height = tlx::integer_log2_ceil(sigma-1);

    for(size_t i = 0; i < height; i++) {
        m_bits.emplace_back(thrill::api::ReadBinary<uint64_t>(
            ctx, filename + "*.lv_" + std::to_string(i+1)));
    }
}

WaveletTree::~WaveletTree() {
}

void WaveletTree::save(
    thrill::Context& ctx,
    const std::string& filename) const {

    // write each level of the WT to file
    for(size_t i = 0; i < m_bits.size(); i++) {
        m_bits[i].WriteBinary(
            filename + ".lv_" + std::to_string(i+1));
    }

    if(ctx.my_rank() == 0) {
        // write histogram to file
        m_hist.save(filename + ".hist");

        // write text length to file
        {
            binary::FileWriter w(filename + ".len");
            w.write(m_len);
        }
    }
}
