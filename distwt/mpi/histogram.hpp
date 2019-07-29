#pragma once

#include <distwt/common/histogram.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/util.hpp>
#include <distwt/mpi/types.hpp>

#include <algorithm>
#include <tlx/math/integer_log2.hpp>

template<typename sym_t>
class Histogram : public HistogramBase<sym_t, idx_t> {
public:
    inline Histogram() {
    }

    inline Histogram(
        MPIContext& ctx,
        const FilePartitionReader<sym_t>& input,
        const size_t rdbufsize) {

        compute_histogram(ctx, input, rdbufsize);
    }

    inline Histogram(const std::string& filename) { // load from file
        this->load(filename);
    }

private:
    // base implementation for arbritrary alphabets
    inline void compute_histogram(
        MPIContext& ctx,
        const FilePartitionReader<sym_t>& input,
        const size_t rdbufsize) {

        {
            // compute local histogram
            std::unordered_map<sym_t, idx_t> local_hist;

            input.process_local([&](sym_t c){
                auto it = local_hist.find(c);
                if(it != local_hist.end()) {
                    ++it->second;
                } else {
                    local_hist.emplace(c, 1);
                }
            }, rdbufsize);

            // distribute using tree-like communication
            {
                std::vector<sym_t> buf_syms;
                std::vector<idx_t> buf_occs;

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

                            auto r = ctx.template probe<sym_t>(ln);
                            ctx.recv(buf_syms, r.size, ln);
                            ctx.recv(buf_occs, r.size, ln);

                            // accumulate
                            for(size_t i = 0; i < r.size; i++) {
                                const auto c = buf_syms[i];
                                const auto occ = buf_occs[i];

                                auto it = local_hist.find(c);
                                if(it != local_hist.end()) {
                                    it->second += occ;
                                } else {
                                    local_hist.emplace(c, occ);
                                }
                            }
                        } else {
                            // even - send to right neighbor
                            const size_t rn = std::min(rank + d, p-1);
                            if(rn != rank) {
                                extract_map(local_hist, buf_syms, buf_occs);
                                ctx.send(buf_syms, rn);
                                ctx.send(buf_occs, rn);
                            } else {
                                // would send to self - skip!
                            }
                        }
                    } else {
                        // idle - do nothing
                    }
                }

                // top-down
                for(size_t _lv = logp; _lv > 0; _lv--) {
                    const size_t lv = _lv - 1;
                    const size_t d = 1ULL << lv;
                    const size_t mask = d - 1;

                    // on the bottom level, every worker is active
                    // the last worker is always active
                    // otherwise, activity is determined by applying the level mask
                    if((lv == 0) || last || ((rank & mask) == mask)) {
                        // active - send or receive based on level rank
                        const size_t lv_rank = rank >> lv;
                        if(lv_rank & 1ULL) {
                            // odd - send to left neighbor
                            const size_t ln = (lv_rank - 1) * d + mask;

                            extract_map(local_hist, buf_syms, buf_occs);
                            ctx.send(buf_syms, ln);
                            ctx.send(buf_occs, ln);
                        } else {
                            // even - receive from right neighbor
                            const size_t rn = std::min(rank + d, p-1);
                            if(rn != rank) {
                                auto r = ctx.template probe<sym_t>(rn);
                                ctx.recv(buf_syms, r.size, rn);
                                ctx.recv(buf_occs, r.size, rn);

                                // update
                                local_hist.clear();
                                for(size_t i = 0; i < r.size; i++) {
                                    local_hist.emplace(buf_syms[i], buf_occs[i]);
                                }
                            } else {
                                // would send to self - skip!
                            }
                        }
                    } else {
                        // idle - do nothing
                    }
                }
            }

            // compute entries
            for(const auto& it : local_hist) {
                this->m_entries.emplace_back(it.first, it.second);
            }
        }

        // sort entries
        std::sort(this->m_entries.begin(), this->m_entries.end(),
            [](const typename HistogramBase<sym_t, idx_t>::entry_t& a,
               const typename HistogramBase<sym_t, idx_t>::entry_t& b){
                return a.first < b.first;
            });

        /*
        if(ctx.rank() == 0) {
            ctx.cout() << "HISTOGRAM:" << std::endl;
            for(const auto& e : m_entries) {
                ctx.cout() << e.first << " -> " << e.second << std::endl;
            }
        }
        */
    }
};

// specialization for 8-bit alphabets
template<>
inline void Histogram<uint8_t>::compute_histogram(
    MPIContext& ctx,
    const FilePartitionReader<uint8_t>& input,
    const size_t rdbufsize) {

    constexpr size_t SIGMA_MAX = 256ULL;

    // compute local histogram
    size_t local_hist[SIGMA_MAX] = {0};
    input.process_local([&](uint8_t c){
        ++local_hist[c];
    }, rdbufsize);

    // distribute
    size_t hist[SIGMA_MAX] = {0};
    ctx.all_reduce(local_hist, hist, SIGMA_MAX);

    // extract nonzero entries
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(hist[c] > 0) {
            m_entries.emplace_back((uint8_t)c, hist[c]);
        }
    }
}
