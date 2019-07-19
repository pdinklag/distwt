#include <tlx/math/div_ceil.hpp>
#include <mpi.h>

#include <distwt/common/util.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

FilePartitionReader::FilePartitionReader(
    const MPIContext& ctx,
    const std::string& filename,
    const size_t prefix)
    : m_ctx(&ctx),
      m_filename(filename),
      m_rank(ctx.rank()),
      m_extracted(false) {

    m_total_size = std::min(util::file_size(m_filename) / sizeof(symbol_t), prefix);
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

        // init buffer
        std::vector<symbol_t> buf(bufsize);

        // open global file for read and seek
        MPI_File fr;
        MPI_File_open(
            m_ctx->comm(),
            m_filename.c_str(),
            MPI_MODE_RDONLY,
            MPI_INFO_NULL,
            &fr);

        MPI_File_seek(fr, m_local_offset * sizeof(symbol_t), MPI_SEEK_SET);

        // open local file for write
        MPI_File fw;
        MPI_File_open(
            MPI_COMM_SELF,
            m_local_filename.c_str(),
            MPI_MODE_WRONLY | MPI_MODE_CREATE,
            MPI_INFO_NULL,
            &fw);

        // copy from global file to local file
        MPI_Status status;
        size_t left = m_local_num;

        while(left) {
            const size_t num = std::min(bufsize, left);
            MPI_File_read(fr, buf.data(), num, mpi_type<symbol_t>::id(), &status);
            MPI_File_write(fw, buf.data(), num, mpi_type<symbol_t>::id(), &status);
            left -= num;
        }

        // close files
        MPI_File_close(&fw);
        MPI_File_close(&fr);

        m_extracted = true;
        return true;
    } else {
        return false;
    }
}

void FilePartitionReader::process_local(
    std::function<void(symbol_t)> func, size_t bufsize) const {

    // open stream and seek position
    MPI_File f;

    if(m_extracted) {
        // part was extracted - open local file
        MPI_File_open(
            MPI_COMM_SELF,
            m_local_filename.c_str(),
            MPI_MODE_RDONLY,
            MPI_INFO_NULL,
            &f);
    } else {
        // open original file and seek position
        MPI_File_open(
            m_ctx->comm(),
            m_filename.c_str(),
            MPI_MODE_RDONLY,
            MPI_INFO_NULL,
            &f);

        MPI_File_seek(f, m_local_offset * sizeof(symbol_t), MPI_SEEK_SET);
    }

    // process
    {
        // initialize read buffer
        std::vector<symbol_t> buf(bufsize);
        MPI_Status status;

        size_t left = m_local_num;

        while(left) {
            const size_t num = std::min(bufsize, left);
            MPI_File_read(f, buf.data(), num, mpi_type<symbol_t>::id(), &status);

            for(size_t i = 0; i < num; i++) {
                func(buf[i]);
            }

            left -= num;
        }
    }

    // close file
    MPI_File_close(&f);
}
