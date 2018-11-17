#include <fstream>
#include <iostream>

#include <tlx/math/div_ceil.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

FilePartitionReader::FilePartitionReader(
    const MPIContext& ctx,
    const std::string& filename)
    : m_filename(filename), m_rank(ctx.rank()), m_extracted(false) {

    m_total_size = util::file_size(m_filename);
    m_size_per_worker = tlx::div_ceil(m_total_size, size_t(ctx.num_workers()));

    m_local_offset = m_size_per_worker * size_t(ctx.rank());
    const size_t local_end = std::min(
        m_local_offset + m_size_per_worker, m_total_size);

    m_local_num = local_end - m_local_offset;
}

bool FilePartitionReader::extract_local(
    const std::string& filename, size_t bufsize) {

    if(!m_extracted) {
        m_local_filename = filename + ".part." + std::to_string(m_rank);
        {
            // init write buffer
            char *buf = new char[bufsize];
            size_t buf_pos = 0;

            // extract local part
            {
                std::ofstream out(m_local_filename, std::ios::binary);
                process_local([&](unsigned char c){
                    buf[buf_pos++] = (char)c;

                    // write overflowing buffer
                    if(buf_pos >= bufsize) {
                        out.write(buf, bufsize);
                        buf_pos = 0;
                    }
                }, bufsize);

                // write remainder
                if(buf_pos > 0) {
                    out.write(buf, buf_pos);
                }
            }

            // clean up
            delete[] buf;
        }

        m_extracted = true;
        return true;
    } else {
        return false;
    }
}

void FilePartitionReader::process_local(
    std::function<void(unsigned char)> func, size_t bufsize) const {

    // initialize read buffer
    char *buf = new char[bufsize];
    size_t buf_count = 0, buf_pos = 0;

    {
        // open stream and seek position
        std::ifstream in;

        if(m_extracted) {
            // part was extracted - open local file
            in = std::ifstream(m_local_filename, std::ios::binary);
        } else {
            // open original file and seek position
            in = std::ifstream(m_filename, std::ios::binary);
            in.seekg(m_local_offset);
        }

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
    }

    // clean up
    delete[] buf;
}
