#pragma once

#include <tlx/math/div_ceil.hpp>
#include <mpi.h>

#include <functional>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>

template<typename sym_t>
class FilePartitionReader {
private:
    const MPIContext* m_ctx;

    std::string m_filename;
    size_t m_total_size, m_size_per_worker;

    size_t m_rank, m_local_offset, m_local_num;

    bool m_extracted;
    std::string m_local_filename;

    bool m_buffered;
    std::vector<sym_t> m_buffer;

public:
    inline FilePartitionReader(
        const MPIContext& ctx,
        const std::string& filename,
        const size_t prefix = SIZE_MAX)
        : m_ctx(&ctx),
          m_filename(filename),
          m_rank(ctx.rank()),
          m_extracted(false),
          m_buffered(false) {

        const size_t w = sizeof(sym_t);
        const size_t filesize = util::file_size(m_filename);
        const size_t mod = filesize % w;

        if(mod) {
            ctx.cout_master() << "Chopping off " << mod
                << " bytes from input file to fit symbols of width " << w
                << " (" << filesize << " % " << w << " = " << mod << ")"
                << std::endl;
        }
        
        m_total_size = std::min(filesize / w, prefix / w);
        m_size_per_worker = tlx::div_ceil(m_total_size, size_t(ctx.num_workers()));

        m_local_offset = m_size_per_worker * size_t(ctx.rank());
        const size_t local_end = std::min(
            m_local_offset + m_size_per_worker, m_total_size);

        m_local_num = local_end - m_local_offset;
    }

    inline const std::string& filename() const { return m_filename; }
    inline size_t total_size() const { return m_total_size; }
    inline size_t size_per_worker() const { return m_size_per_worker; }

    inline size_t local_offset() const { return m_local_offset; }
    inline size_t local_num() const { return m_local_num; }

    bool extract_local(const std::string& local_filename, size_t bufsize) {
        if(!m_extracted) {
            m_local_filename = local_filename + ".part." + std::to_string(m_rank);

            // init buffer
            std::vector<sym_t> buf(bufsize);

            // open global file for read and seek
            MPI_File fr;
            MPI_File_open(
                m_ctx->comm(),
                m_filename.c_str(),
                MPI_MODE_RDONLY,
                MPI_INFO_NULL,
                &fr);

            MPI_File_seek(fr, m_local_offset * sizeof(sym_t), MPI_SEEK_SET);

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
                MPI_File_read(fr, buf.data(), num, mpi_type<sym_t>::id(), &status);
                MPI_File_write(fw, buf.data(), num, mpi_type<sym_t>::id(), &status);
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

    void process_local(
        std::function<void(sym_t)> func, size_t bufsize) const {

        if(m_buffered) {
            for(sym_t x : m_buffer) {
                func(x);
            }
        } else {

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

                MPI_File_seek(f, m_local_offset * sizeof(sym_t), MPI_SEEK_SET);
            }

            // process
            {
                // initialize read buffer
                std::vector<sym_t> buf(bufsize);
                MPI_Status status;

                size_t left = m_local_num;

                while(left) {
                    const size_t num = std::min(bufsize, left);
                    MPI_File_read(f, buf.data(), num, mpi_type<sym_t>::id(), &status);

                    for(size_t i = 0; i < num; i++) {
                        func(buf[i]);
                    }

                    left -= num;
                }
            }

            // close file
            MPI_File_close(&f);
            
        }
    }

    void buffer(size_t bufsize) {
        if(!m_buffered) {
            m_buffer.reserve(m_local_num);
            
            process_local([&](const sym_t x){
                m_buffer.push_back(x);
            }, bufsize);

            m_buffered = true;
        }
    }

    void free() {
        if(m_buffered) {
            m_buffer.clear();
            m_buffer.shrink_to_fit();
            m_buffered = false;
        }
    }
};
