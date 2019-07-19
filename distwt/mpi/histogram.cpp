#include <unordered_map>

#include <tlx/math/integer_log2.hpp>

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

    // distribute using tree-like communication
    {
        const size_t rank = ctx.rank();
        const size_t p = ctx.num_workers();
        const bool last = (rank == p-1);
        const size_t logp = tlx::integer_log2_ceil(p);

        // bottom-up
        for(size_t lv = 0; lv < logp; lv++) {
            const size_t d = 1ULL << lv;
            const size_t mask = d - 1;

            // on the bottom level, every worker is active
            // the last worker is always active
            // otherwise, activity is determined by applying the level mask
            if((lv == 0) || last || ((rank & mask) == mask)) {
                // active - send or receive based on level rank
                const size_t lv_rank = rank >> lv;
                if(lv_rank & 1ULL) {
                    // odd - receive from left neighbor
                    const size_t ln = (lv_rank - 1) * d + mask;
                    ctx.cout() << "on level " << lv << ": RECV from " << ln << std::endl;
                } else {
                    // even - send to right neighbor
                    const size_t rn = std::min(rank + d, p-1);
                    if(rn != rank) {
                        ctx.cout() << "on level " << lv << ": SEND to " << rn << std::endl;
                    } else {
                        // would send to self - skip!
                        ctx.cout() << "on level " << lv << ": SKIP" << std::endl;
                    }
                }
            } else {
                // idle - do nothing
                ctx.cout() << "on level " << lv << ": IDLE" << std::endl;
            }
        }
    }
}

// for 8-bit alphabets
/*
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
*/

Histogram::Histogram(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const size_t rdbufsize) {

    compute_histogram<symbol_t>(m_entries, ctx, input, rdbufsize);
}
