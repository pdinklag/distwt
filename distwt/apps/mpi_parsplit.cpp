#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/integer_log2.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/parallel_split.hpp>
#include <distwt/mpi/wt_construct_sequential.hpp>
#include <distwt/mpi/wt_nodebased.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

#include <distwt/mpi/result.hpp>

void recursiveWT(
    WaveletTree::bits_t& bits,
    MPIContext& ctx,
    const size_t node_id,
    std::vector<esym_t>& text,
    const size_t a,
    const size_t b) {

    if(a == b) return;

    ctx.cout_master() << "Processing node " << node_id << " using "
        << ctx.num_workers() << " worker(s) ..." << std::endl;

    if(ctx.num_workers() == 1) {
        // we are left with only one worker
        // this may happen based on the balance of 0/1 bits in a bit vector
        // simply use a sequential algorithm to compute the remaining WT

        // allocate buffer
        esym_t* buffer = new esym_t[text.size()];

        // compute sequential
        wt_construct_nodebased_sequential(
            bits,
            ctx,
            node_id,
            text.data(),
            text.size(),
            a, b,
            buffer);

        // clean up and return
        delete[] buffer;
        return;
    }

    const size_t m = (a + b) / 2;

    // compute node bit vector
    const size_t n = text.size();
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

    if(a < m || m+1 < b) {
        // perform parallel split
        const size_t split = parallel_str_split(
            ctx,
            text,
            [m](const esym_t& x){return (x > m);},
            z, n-z,
            (int)node_id);

        // create communicators for left and right groups
        MPI_Comm parent_comm = ctx.comm();
        MPI_Group target_group_l, target_group_r;
        MPI_Comm target_comm_l, target_comm_r;
        {
            MPI_Group parent_group;
            MPI_Comm_group(parent_comm, &parent_group);

            const size_t num_l = split;
            int ranks_l[num_l];
            for(size_t i = 0; i < num_l; i++) {
                ranks_l[i] = int(i);
            }

            // left
            MPI_Group_incl(parent_group, num_l, ranks_l, &target_group_l);
            MPI_Comm_create(parent_comm, target_group_l, &target_comm_l);

            // right
            MPI_Group_excl(parent_group, num_l, ranks_l, &target_group_r);
            MPI_Comm_create(parent_comm, target_group_r, &target_comm_r);
        }

        // recurse in respective group
        if(text.size() > 0) {
            if(ctx.rank() < split) {
                // recurse with left child in left group
                ctx.set_comm(target_comm_l);
                recursiveWT(
                    bits,
                    ctx,
                    2ULL * node_id,
                    text,
                    a, m);
            } else {
                // recurse with right child in right group
                ctx.set_comm(target_comm_r);
                recursiveWT(
                    bits,
                    ctx,
                    2ULL * node_id + 1,
                    text,
                    m+1, b);
            }

            // restore communicator
            ctx.set_comm(parent_comm);
        } else {
            ctx.cout() << "idling after computation of node " << node_id
                << " ..." << std::endl;
        }

        // synchronize
        ctx.synchronize();

        // free temporary communicators
        if(target_comm_l != MPI_COMM_NULL) MPI_Comm_free(&target_comm_l);
        if(target_comm_r != MPI_COMM_NULL) MPI_Comm_free(&target_comm_r);
        MPI_Group_free(&target_group_l);
        MPI_Group_free(&target_group_r);
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

    size_t prefix = SIZE_MAX; // default to whole file
    cp.add_bytes('p', "prefix", prefix, "Only process prefix of input file.");

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Init MPI
    MPIContext ctx(&argc, &argv);
    const double t0 = ctx.time();

    // Determine input partition
    FilePartitionReader input(ctx, input_filename, prefix);
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

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    ctx.cout_master() << "Compute effective transformation ..." << std::endl;
    std::vector<esym_t> etext(local_num);
    {
        size_t i = 0;
        ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
    }

    // recursive WT
    const double t1 = ctx.time();
    const double time_input = t1 - t0;

    ctx.cout_master() << "Compute WT ..." << std::endl;
    auto wt_nodes = WaveletTreeNodebased(hist,
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        bits.resize(wt.num_nodes());

        recursiveWT(
            bits,
            ctx,
            1ULL, // root
            etext, // text
            0ULL, wt.num_nodes()); // alphabet interval
    });

    // Clean up
    etext.clear();
    etext.shrink_to_fit();

    // Synchronize
    ctx.cout_master() << "Done computing " << wt_nodes.num_nodes()
        << " nodes. Synchronizing ..." << std::endl;
    ctx.synchronize();

    // Convert to level-wise representation
    WaveletTreeLevelwise wt = wt_nodes.merge(ctx, input, hist, true);

    const double time_construct = ctx.time() - t1;

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WT to disk ..." << std::endl;

        hist.save(output + "." + WaveletTreeBase::histogram_extension());
        wt.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-dd", ctx, input, wt.sigma(), time_input, time_construct);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
    return 0;
}
