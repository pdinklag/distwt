#include <distwt/common/bv64.hpp>
#include <distwt/mpi/wm.hpp>

#include <iomanip>
#include <mpi.h>

void WaveletMatrix::save(
    const MPIContext& ctx,
    const std::string& output) {

    // save WM levels
    for(size_t level = 0; level < height(); level++) {
        // construct local filename
        std::string filename;
        {
            std::ostringstream ss;
            ss << output << std::setw(4) << std::setfill('0')
                << ctx.rank() << '.'
                << WaveletTreeBase::level_extension(level);
            filename = ss.str();
        }

        // open file
        MPI_File f;
        MPI_File_open(
            MPI_COMM_SELF,
            filename.c_str(),
            MPI_MODE_WRONLY | MPI_MODE_CREATE,
            MPI_INFO_NULL,
            &f);

        // write
        MPI_Status status;

        bv64_t bitbuf;
        size_t x = 0;

        const auto& bv = m_bits[level];
        for(size_t i = 0; i < bv.size(); i++) {
            bitbuf[63ULL - (x++)] = bv[i];
            if(x >= 64ULL) {
                uint64_t ull = bitbuf.to_ullong();
                MPI_File_write(f, &ull, 1, MPI_LONG_LONG, &status);

                x = 0;
            }
        }

        if(x > 0) {
            uint64_t ull = bitbuf.to_ullong();
            MPI_File_write(f, &ull, 1, MPI_LONG_LONG, &status);
        }

        // close file
        MPI_File_close(&f);
    }
}
