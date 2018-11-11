#include <distwt/mpi/histogram.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

constexpr size_t SIGMA_MAX = 256ULL;

Histogram::Histogram(
    const MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    // compute local histogram
    size_t local_hist[SIGMA_MAX] = {0};
    input.process_local([&](unsigned char c){
        ++local_hist[c];
    }, rdbufsize);

    // distribute
    size_t hist[SIGMA_MAX] = {0};
    MPI_Allreduce(local_hist, hist, SIGMA_MAX,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    // extract nonzero entries
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(hist[c] > 0) {
            m_entries.emplace_back((symbol_t)c, hist[c]);
        }
    }
}
