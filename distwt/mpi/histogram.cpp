#include <distwt/mpi/histogram.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

// FIXME: what to do for sigma >= 256 ??
constexpr size_t SIGMA_MAX = 256ULL;

Histogram::Histogram(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    // compute local histogram
    size_t local_hist[SIGMA_MAX] = {0};
    input.process_local([&](symbol_t c){
        ++local_hist[c];
    }, rdbufsize);

    // distribute
    size_t hist[SIGMA_MAX] = {0};
    ctx.all_reduce(local_hist, hist, SIGMA_MAX);

    // extract nonzero entries
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(hist[c] > 0) {
            m_entries.emplace_back((symbol_t)c, hist[c]);
        }
    }
}
