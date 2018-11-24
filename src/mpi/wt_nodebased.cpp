#include <distwt/mpi/wt_nodebased.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

#include <distwt/mpi/prefix_sum.hpp>

WaveletTreeLevelwise WaveletTreeNodebased::merge(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const HistogramBase& hist,
    bool discard) {

    return WaveletTreeLevelwise(hist, // TODO: avoid recomputations!
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        bits.resize(height());
        bits[0] = m_bits[0]; // simply copy root

        if(discard) {
            m_bits[0].clear();
            m_bits[0].shrink_to_fit();
        }

        // Part 1 - Distribute local offsets for all nodes
        ctx.cout_master() << "Distributing node prefix sums ..." << std::endl;

        const size_t num_nodes = wt.num_nodes();
        std::vector<size_t> local_node_offs(num_nodes);
        {
            // compute prefix sum of local node sizes
            for(size_t i = 0; i < num_nodes; i++) {
                local_node_offs[i] = m_bits[i].size();
            }
            prefix_sum(ctx, local_node_offs);
        }

        // Part 2 - Distribute bits in a balanced manner
        ctx.cout_master() << "Distributing level bit vectors ..." << std::endl;
        {
            // prepare send / receive vectors
            const size_t bits_per_worker = input.size_per_worker();

            // note: nothing to do for the root level!
            for(size_t level = 1; level < wt.height(); level++) {
                ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

                // do this level by level
                const size_t num_level_nodes = 1ULL << level;
                const size_t first_level_node = num_level_nodes;

                // messages to self
                // buffer needs to be of fixed size to avoid destructor calls
                std::vector<bv_interval_msg_t> msg_self(num_level_nodes);

                std::vector<bv_interval_msg_t> msg_outbox(2ULL * num_level_nodes);
                size_t msg_out_num = 0;

                // determine which bits from this worker go to other workers
                size_t level_node_offs = 0;
                for(size_t i = 0; i < num_level_nodes; i++) {
                    const size_t node_id = first_level_node + i;

                    auto& bv = m_bits[node_id-1];
                    if(bv.size() > 0) {
                        const size_t glob_node_offs =
                            level_node_offs + local_node_offs[node_id-1];

                        // sender
                        auto send = [&](const size_t target,
                            const bv_t& bv,size_t p, size_t q){

                            const size_t num = q - p + 1;
                            const size_t local_p = p - glob_node_offs;
                            const size_t local_q = local_p + num - 1;

                            /*
                            ctx.cout() << "Send [" << p << "," << q << "] ("
                                << "local [" << local_p << "," << local_q << "]) ("
                                << num << " bits) of level " << (level+1)
                                << " (node " << node_id << ")"
                                << " to #" << target << std::endl;
                            */

                            if(target == ctx.rank()) {
                                // send to self
                                encode_bv_interval_msg(
                                    msg_self[i], bv, local_p, local_q, p, q);
                            } else {
                                auto& msg = msg_outbox[msg_out_num++];

                                encode_bv_interval_msg(
                                    msg, bv, local_p, local_q, p, q);

                                ctx.isend(msg.data, msg.size, target, level);
                            }
                        };

                        // find range of the local node's bit vector
                        // in global level's bit vector
                        const size_t p = glob_node_offs;
                        const size_t q = p + bv.size() - 1;

                        // find target workers for my local range
                        // LEMMA: there can be at most two of them, because the node
                        //        has at most bits_per_worker bits
                        const size_t target1 = p / bits_per_worker;
                        const size_t target2 = q / bits_per_worker;

                        if(target1 == target2) {
                            // send bv[p..q] bits to target
                            send(target1, bv, p, q);
                        } else {
                            // split it up at the boundary m
                            const size_t m = target2 * bits_per_worker;

                            // send bv[p..m-1] to target1
                            send(target1, bv, p, m-1);

                            // send bv[m..q] to target2
                            send(target2, bv, m, q);
                        }

                        if(discard) {
                            // discard node bit vector
                            bv.clear();
                            bv.shrink_to_fit();
                        }
                    }

                    // advance
                    level_node_offs += m_node_sizes[node_id-1];
                }

                // allocate level bv
                bits[level].resize(input.local_num());

                // receiver
                size_t bits_received = 0;

                const size_t glob_offs = ctx.rank() * bits_per_worker;
                auto recv = [&](const bv_interval_msg_t& msg, const size_t source){
                    // DEBUG
                    /*
                    const size_t p = msg.data[0];
                    const size_t q = msg.data[1];
                    const size_t num = q-p+1;
                    ctx.cout() << "Receive [" << p << "," << q
                        << "] (" << num << " bits) of level " << (level+1)
                        << " from #" << source
                        << " (glob_offs = " << glob_offs << ")" << std::endl;
                    */

                    bits_received += decode_bv_interval_msg(
                        msg, bits[level], glob_offs);

                    // DEBUG
                    /*
                    ctx.cout() << "got " << bits_received << " / "
                        << input.local_num() << " bits" << std::endl;
                    */
                };

                // receive messages sent to self first
                {
                    for(auto& msg : msg_self) {
                        if(msg) recv(msg, ctx.rank());
                    }
                }

                // now receive messages from other nodes until
                // local_num bits have been received
                while(bits_received < input.local_num()) {
                    auto r = ctx.template probe<uint64_t>((int)level);

                    if(r.size > 0) {
                        bv_interval_msg_t msg(r.size);
                        ctx.recv(msg.data, msg.size, r.sender, level);
                        recv(msg, r.sender);
                    }
                }

                // DEBUG
                ctx.synchronize();
                /*
                ctx.cout() << "level " << (level+1) << ": " << levels[level] << std::endl;
                */
            }
        }

        if(discard) {
            m_bits.clear();
            m_bits.shrink_to_fit();
        }
    });
}
