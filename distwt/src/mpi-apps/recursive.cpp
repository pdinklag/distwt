#include <bitset>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/bit_vector.hpp>

#include <distwt/common/result.hpp>

using bits_t = std::vector<bv_t>;

void recursiveWT(
    bits_t& bits,
    const MPIContext& ctx,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer) {

    if(a == b) return;

    const size_t m = (a + b) / 2;

    // compute node bit vector
    size_t z = 0;
    {
        auto& bv = bits[node_id-1];
        bv.resize(n);

        for(size_t i = 0; i < n; i++) {
            if(text[i] <= m) {
                bv[i] = 0;
                ++z;
            } else {
                bv[i] = 1;
            }
        }
    }

    // only continue if needed
    if(a < m || m+1 < b) {
        // compute permutation of text in buffer
        {
            esym_t* pl = buffer;
            esym_t* pr = buffer + z;

            for(size_t i = 0; i < n; i++) {
                auto c = text[i];
                if(c <= m) {
                    *pl++ = c;
                } else {
                    *pr++ = c;
                }
            }
        }

        // recurse on left part
        recursiveWT(
            bits,
            ctx,
            2ULL * node_id, // left child
            buffer, z,
            a, m,
            text);

        // recurse on right part
        recursiveWT(
            bits,
            ctx,
            2ULL * node_id + 1ULL, // right child
            buffer + z, n - z,
            m+1, b,
            text + z);
    }
}

int main(int argc, char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;

    size_t rdbufsize = 0; // default to local input size
    cp.add_bytes('r', "rbuf", rdbufsize, "File read buffer size.");

    std::string local_filename("");
    cp.add_string('l', "local", local_filename, "Name of local part file.");

    std::string output("");
    cp.add_string('o', "output", output, "Name of output file.");

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Init MPI
    MPIContext ctx(&argc, &argv);
    const double t0 = util::time();

    // Determine input partition
    FilePartitionReader input(ctx, input_filename);
    const size_t local_num = input.local_num();

    if(rdbufsize == 0) {
        rdbufsize = local_num;
    }

    if(local_filename.length() > 0) {
        // Extract local part
        ctx.cout_master() << "Extract partition to "
            << local_filename << " ..." << std::endl;

        input.extract_local(local_filename, rdbufsize);

        // Synchronize
        ctx.cout_master() << "Synchronizing ..." << std::endl;
        ctx.synchronize();
    }

    // Compute histogram
    ctx.cout_master() << "Compute histogram ..." << std::endl;
    Histogram hist(ctx, input, rdbufsize);

    // prepare WT
    WaveletTreeBase wt(hist);
    const size_t num_nodes = wt.num_nodes();
    bits_t nodes(num_nodes);

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    {
        ctx.cout_master() << "Compute effective transformation ..." << std::endl;
        esym_t* etext = new esym_t[local_num];
        {
            size_t i = 0;
            ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
        }

        // recursive WT
        ctx.cout_master() << "Compute WT ..." << std::endl;
        esym_t* buffer = new esym_t[local_num];
        recursiveWT(
            nodes,
            ctx,
            1ULL, // root
            etext, local_num, // text
            0ULL, num_nodes, // interval
            buffer); // work buffer

        // Clean up
        delete[] buffer;
        delete[] etext;
    }

    // DEBUG
    /*
    for(size_t node = 0; node < wt.num_nodes(); node++) {
        ctx.cout() << "node " << (node+1) << ": " << nodes[node] << std::endl;
    }
    */

    // Synchronize
    ctx.cout_master() << "Done computing " << nodes.size()
        << " nodes. Synchronizing ..." << std::endl;
    ctx.synchronize();

    // Convert to level-wise representation
    // balanced, so that each worker has an equal amount of bits for each level
    bits_t levels(wt.height());

    // simply copy first level from root note
    levels[0] = nodes[0];

    // Part 1 - Distribute local offsets for all nodes
    {
        ctx.cout_master() << "Distributing node prefix sums ..." << std::endl;

        auto& node_sizes = wt.node_sizes();
        std::vector<size_t> local_node_offs(num_nodes);
        {
            // compute local node sizes
            std::vector<size_t> sz(num_nodes);
            for(size_t i = 0; i < num_nodes; i++) {
                sz[i] = nodes[i].size();
            }

            // send sizes to workers with higher rank
            for(size_t to = ctx.rank() + 1; to < ctx.num_workers(); to++) {
                ctx.send(sz, to);
            }

            // receive sizes from workers with lower rank
            for(size_t from = 0; from < ctx.rank(); from++) {
                ctx.recv(sz, num_nodes, from);

                // compute prefix sum
                for(size_t i = 0; i < num_nodes; i++) {
                    local_node_offs[i] += sz[i];
                }
            }
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

                    auto& bv = nodes[node_id-1];
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

                        // discard node bit vector
                        bv.clear();
                        bv.shrink_to_fit();
                    }

                    // advance
                    level_node_offs += node_sizes[node_id-1];
                }

                // allocate level bv
                levels[level].resize(local_num);

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
                        msg, levels[level], glob_offs);

                    // DEBUG
                    /*
                    ctx.cout() << "got " << bits_received << " / "
                        << local_num << " bits" << std::endl;
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
                while(bits_received < local_num) {
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
    }

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WT to disk ..." << std::endl;

        // save histogram
        hist.save(output + "." + WaveletTreeBase::histogram_extension());

        // save WT levels
        for(size_t level = 0; level < wt.height(); level++) {
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

            std::bitset<64> bitbuf;
            size_t x = 0;

            const auto& bv = levels[level];
            for(size_t i = 0; i < local_num; i++) {
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

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // Gather stats
    const double dt = util::time() - t0;
    auto traffic = ctx.gather_traffic_data();
    Result result(
        "mpi-recursive",
        ctx.num_nodes(),
        ctx.num_workers_per_node(),
        input.filename(),
        input.total_size(),
        dt,
        traffic.total_incl_est()
    );

    ctx.cout_master() << result.sqlplot() << std::endl;
    ctx.cout_master() << result.readable() << std::endl;
    return 0;
}
