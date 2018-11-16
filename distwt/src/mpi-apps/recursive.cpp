#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/div_ceil.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>

using bv_t = std::vector<bool>;
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

    size_t rdbufsize = 1024ULL * 1024ULL; // default to 1Mi
    cp.add_bytes('r', "rbuf", rdbufsize, "File read buffer size.");

    std::string local_filename("");
    cp.add_string('l', "local", local_filename, "Name of local part file.");

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Init MPI
    MPIContext ctx(&argc, &argv);
    ctx.cout_master() <<
        "Starting computation with " << ctx.num_workers() << " workers ..." << std::endl;

    // Determine input partition
    FilePartitionReader input(ctx, input_filename);

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

    // FIXME: Right now, nothing is externalized, therefore we need some
    // minimum amount of memory to process texts.
    {
        const size_t required_memory =
            // bit vectors
            (wt.height() * input.local_num()) / 8ULL +
            // 2x local effective transformation
            2ULL * input.local_num() * (sizeof(esym_t) / 8ULL);

        ctx.cout_master() << "required_memory: " <<
            required_memory << " bytes" << std::endl;
    }

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    {
        ctx.cout_master() << "Compute effective transformation ..." << std::endl;
        esym_t* etext = new esym_t[input.local_num()];
        {
            size_t i = 0;
            ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
        }

        // recursive WT
        ctx.cout_master() << "Compute WT ..." << std::endl;
        esym_t* buffer = new esym_t[input.local_num()];
        recursiveWT(
            nodes,
            ctx,
            1ULL, // root
            etext, input.local_num(), // text
            0ULL, num_nodes, // interval
            buffer); // work buffer

        // Clean up
        delete[] buffer;
        delete[] etext;
    }

    // Synchronize
    ctx.cout_master() << "Done computing " << nodes.size()
        << " nodes. Synchronizing ..." << std::endl;
    ctx.synchronize();

    // Distribute local offsets for all nodes
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
                MPI_Send(sz.data(), sz.size(), MPI_LONG_LONG, (int)to, 0, MPI_COMM_WORLD);
            }

            // receive sizes from workers with lower rank
            MPI_Status status;
            for(size_t from = 0; from < ctx.rank(); from++) {
                MPI_Recv(sz.data(), num_nodes, MPI_LONG_LONG, (int)from, 0, MPI_COMM_WORLD, &status);

                // compute prefix sum
                for(size_t i = 0; i < num_nodes; i++) {
                    local_node_offs[i] += sz[i];
                }
            }
        }

        // Convert to level-wise representation
        // balanced, so that each worker has an equal amount of bits for each level
        {
            // prepare send / receive vectors
            const size_t bits_per_worker = input.local_num();

            // note: nothing to do for the root level!
            for(size_t level = 1; level < wt.height(); level++) {
                // do this level by level
                const size_t num_level_nodes = 1ULL << level;
                const size_t first_level_node = num_level_nodes;

                // determine which bits from this worker go to other workers
                size_t node_offs = 0;
                for(size_t i = 0; i < num_level_nodes; i++) {
                    const size_t node_id = first_level_node + i;

                    auto& bv = nodes[node_id-1];

                    // find range of the local node's bit vector
                    // in global level's bit vector
                    const size_t my_first = node_offs + local_node_offs[node_id-1];
                    const size_t my_last = my_first + bv.size() - 1;

                    // to each worker, send the correspondig bits from the
                    // global level's bit vector (or zero)
                    for(size_t to = 0; to < ctx.num_workers(); to++) {
                        const size_t first = to * bits_per_worker;
                        const size_t last  = first + bits_per_worker - 1;

                        // is my local range part of the worker's global range?
                        const size_t p = std::max(first, my_first);
                        const size_t q = std::min(last, my_last);
                        if(p < q) {
                            // yep, send corresponding bits
                            // note: also send to self - this makes sure that
                            //       the ordering is correct later

                            // TODO: send bv[p..q]
                            ctx.cout() << "Send [" << p << "," << q
                                << "] of node " << node_id << " to " << to
                                << std::endl;
                        } else {
                            // nope, send nothing
                        }
                    }

                    // discard bit vector
                    bv.clear();
                    bv.shrink_to_fit();

                    // advance
                    node_offs += node_sizes[node_id-1];
                }

                // receive partial bit vectors from all workers (incl. self!)
                // and concatenate
                for(size_t from = 0; from < ctx.num_workers(); from++) {
                    // TODO: receive bv and append to local level bv
                }
            }
        }
    }

    // Exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.exit();
    ctx.cout_master() << "Finished." << std::endl;
    return 0;
}
