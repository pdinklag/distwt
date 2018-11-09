#pragma once

#include <functional>

#include <distwt/mpi/context.hpp>

class FilePartitionReader {
private:
    std::string m_filename;
    size_t m_total_size;

    size_t m_local_offset, m_local_num;

public:
    FilePartitionReader(const MPIContext& ctx, const std::string& filename);

    inline const std::string& filename() const { return m_filename; }
    inline size_t total_size() const { return m_total_size; }

    inline size_t local_offset() const { return m_local_offset; }
    inline size_t local_num() const { return m_local_num; }

    void process_local(
        std::function<void(unsigned char)> func, size_t bufsize) const;
};
