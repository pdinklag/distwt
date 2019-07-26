#pragma once

#include <cassert>

#include <distwt/mpi/wt.hpp>
#include <distwt/mpi/wt_levelwise.hpp>
#include <distwt/mpi/wm.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/bitrev.hpp>
#include <distwt/mpi/uint64_pack_bv64.hpp>

class WaveletTreeLevelwise; // fwd
class WaveletTreeNodebased : public WaveletTree {
public:
    template<typename sym_t>
    inline WaveletTreeNodebased(
        const HistogramBase<sym_t>& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    template<typename sym_t>
    WaveletTreeLevelwise merge(
        MPIContext& ctx,
        const FilePartitionReader<sym_t>& input,
        const HistogramBase<sym_t>& hist,
        bool discard) {

        return WaveletTreeLevelwise(hist, // TODO: avoid recomputations!
            [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
                merge_impl(ctx, bits, wt, input, hist, discard, false);
            });
    }

    template<typename sym_t>
    WaveletMatrix merge_to_matrix(
        MPIContext& ctx,
        const FilePartitionReader<sym_t>& input,
        const HistogramBase<sym_t>& hist,
        bool discard) {

        return WaveletMatrix(hist, // TODO: avoid recomputations!
            [&](WaveletMatrix::bits_t& bits, WaveletMatrix::z_t& z, const WaveletMatrixBase& wm){
                merge_impl(ctx, bits, wm, input, hist, discard, true);

                // compute Z values from histogram
                const size_t sigma = hist.size();
                size_t mask = 1ULL << (height() - 1);

                for(size_t level = 0; level < height(); level++) {
                    size_t num0 = 0;
                    for(size_t i = 0; i < sigma; i++) {
                        if((i & mask) == 0) num0 += hist.entries[i].second;
                    }
                    z[level] = num0;
                    mask >>= 1ULL;
                }
            });
    }

private:
    template<typename sym_t, typename bits_t, typename target_t>
    void merge_impl(
        MPIContext& ctx,
        bits_t& bits,
        const target_t& target,
        const FilePartitionReader<sym_t>& input,
        const HistogramBase<sym_t>& hist,
        bool discard,
        bool bit_reversal) {

        auto node_sizes = WaveletTreeBase::node_sizes(hist);

        bits.resize(this->height());
        bits[0] = m_bits[0]; // simply copy root

        if(discard) {
            m_bits[0].clear();
            m_bits[0].shrink_to_fit();
        }

        // Part 1 - Distribute local offsets for all nodes
        ctx.cout_master() << "Distributing node prefix sums ..." << std::endl;

        const size_t num_nodes = this->num_nodes();
        std::vector<size_t> local_node_offs(num_nodes);
        {
            // compute prefix sum of local node sizes
            for(size_t i = 0; i < num_nodes; i++) {
                local_node_offs[i] = m_bits[i].size();
            }

            ctx.ex_scan(local_node_offs);
        }

        // Part 2 - Distribute bits in a balanced manner
        ctx.cout_master() << "Distributing level bit vectors ..." << std::endl;
        {
            // prepare send / receive vectors
            const size_t bits_per_worker = input.size_per_worker();

            // note: nothing to do for the root level!
            for(size_t level = 1; level < this->height(); level++) {
                ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

                // do this level by level
                const size_t num_level_nodes = 1ULL << level;
                const size_t first_level_node = num_level_nodes;

                // allocate message buffers
                std::vector<uint64_t*> msg_buf;

                // determine which bits from this worker go to other workers
                size_t level_node_offs = 0;
                for(size_t i = 0; i < num_level_nodes; i++) {
                    const size_t node_id = first_level_node +
                        (bit_reversal ? bitrev(i, level) : i);

                    auto& bv = m_bits[node_id-1];
                    if(bv.size() > 0) {
                        const size_t glob_node_offs =
                            level_node_offs + local_node_offs[node_id-1];

                        // map range of the local node's bit vector
                        // in global level's bit vector
                        size_t p = glob_node_offs;
                        const size_t q = p + bv.size();

                        while(p < q) {
                            // determine target
                            const size_t target = p / bits_per_worker;

                            // determine next boundary
                            const size_t x = std::min(
                                (target+1) * bits_per_worker, q);

                            // send interval [p,x) to target
                            const size_t local_offs = p - glob_node_offs;
                            const size_t num = x - p;

                            #ifdef DBG_MERGE
                            ctx.cout() << "Send [" << p << "," << x << ") ("
                                << num << " bits) of level " << (level+1)
                                << " (node " << node_id << ")"
                                << " to #" << target << std::endl;
                            #endif

                            const size_t size =
                                bv64_pack_t::required_bufsize(num)+2;

                            uint64_t* msg = new uint64_t[size];
                            msg[0] = p;
                            msg[1] = num;
                            bv64_pack_t::pack(bv, local_offs, msg+2, num);
                            msg_buf.push_back(msg);

                            ctx.isend(msg, size, target, (int)level);

                            // advance in node
                            p = x;
                        }

                        if(discard) {
                            // discard node bit vector
                            bv.clear();
                            bv.shrink_to_fit();
                        }
                    }

                    // advance in level
                    level_node_offs += node_sizes[node_id-1];
                }

                // allocate level bv
                const size_t local_num = input.local_num();
                bits[level].resize(local_num);

                // receive messages until local_num bits have been received
                const size_t global_offset = ctx.rank() * bits_per_worker;

                size_t num_received = 0;
                while(num_received < local_num) {
                    // probe for message (blocking)
                    auto result = ctx.template probe<uint64_t>((int)level);

                    uint64_t* msg = new uint64_t[result.size];
                    ctx.recv(msg, result.size, result.sender, (int)level);

                    const size_t moffs = msg[0];
                    const size_t mnum = msg[1];

                    assert(result.size == bv64_pack_t::required_bufsize(mnum) + 2);

                    // receive global interval [moffs, moffs+mnum)
                    #ifdef DBG_MERGE
                    ctx.cout() << "receive ["
                        << moffs << ","
                        << moffs + mnum
                        << ") (" << mnum << " bits) from "
                        << result.sender << std::endl;
                    #endif

                    assert(moffs >= global_offset);
                    assert(moffs - global_offset + mnum <= local_num);

                    bv64_pack_t::unpack(
                        msg+2, bits[level], moffs - global_offset, mnum);

                    num_received += mnum;

                    #ifdef DBG_MERGE
                    ctx.cout() << "got " << num_received << " / "
                        << local_num << " bits" << std::endl;
                    #endif
                }

                // this synchronization is necessary in order to maintain the
                // outbox buffer until all messages have been received
                ctx.synchronize();

                // clean up
                for(uint64_t* msg : msg_buf) {
                    delete[] msg;
                }
            }
        }

        if(discard) {
            m_bits.clear();
            m_bits.shrink_to_fit();
        }
    }
};
