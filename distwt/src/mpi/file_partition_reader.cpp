#include <fstream>
#include <iostream>

#include <tlx/math/div_ceil.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

FilePartitionReader::FilePartitionReader(
    const MPIContext& ctx,
    const std::string& filename)
    : m_filename(filename) {

    m_total_size = util::file_size(m_filename);
    const size_t sz_per_worker = tlx::div_ceil(
        m_total_size, size_t(ctx.num_workers()));

    m_local_offset = sz_per_worker * size_t(ctx.rank());
    const size_t local_end = std::min(
        m_local_offset + sz_per_worker, m_total_size);

    m_local_num = local_end - m_local_offset;
}

void FilePartitionReader::process_local(
    std::function<void(unsigned char)> func, size_t bufsize) const {

    // open stream and seek position
    std::ifstream in(m_filename, std::ios::binary);
    in.seekg(m_local_offset);

    // initialize read buffer
    char *buf = new char[bufsize];
    size_t buf_count = 0, buf_pos = 0;

    // process
    for(size_t read = 0; read < m_local_num; read++) {
        // fill underflowing buffer
        if(buf_pos >= buf_count) {
            buf_count = std::min(bufsize, m_local_num - read);
            in.read(buf, buf_count);
            buf_pos = 0;
        }

        func((unsigned char)buf[buf_pos++]);
    }

    // clean up
    delete[] buf;
}
