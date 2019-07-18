#include <unordered_map>

#include <distwt/mpi/histogram.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

// for arbritrary alphabets
template<typename T>
void compute_histogram(
    std::vector<HistogramBase::entry_t>& m_entries,
    MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    // compute local histogram
    std::unordered_map<symbol_t, size_t> local_hist;
    
    input.process_local([&](symbol_t c){
        auto it = local_hist.find(c);
        if(it != local_hist.end()) {
            ++it->second;
        } else {
            local_hist.emplace(c, 1);
        }
    }, rdbufsize);

    // --- TODO: distribute using tree-like communication ---
    // ...
}

// for 8-bit alphabets
template<>
void compute_histogram<unsigned char>(
    std::vector<HistogramBase::entry_t>& m_entries,
    MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    constexpr size_t SIGMA_MAX = 256ULL;

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

Histogram::Histogram(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    compute_histogram<symbol_t>(m_entries, ctx, input, rdbufsize);
}
