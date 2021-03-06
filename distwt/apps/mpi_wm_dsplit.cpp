#include "mpi_launcher.hpp"

#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <distwt/common/util.hpp>
#include <distwt/common/wt_sequential.hpp>

#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/dsplit.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/wt_nodebased.hpp>
#include <distwt/mpi/wm.hpp>

#include <distwt/mpi/result.hpp>

template<typename sym_t>
void recursiveWT(
    WaveletTree::bits_t& bits,
    MPIContext& ctx,
    const size_t node_id,
    std::vector<sym_t>& text,
    const size_t a,
    const size_t b) {

    if(a == b) return;

    ctx.cout_master() << "Processing node " << node_id << " using "
        << ctx.num_workers() << " worker(s) ..." << std::endl;

    if(ctx.num_workers() == 1) {
        // we are left with only one worker
        // this may happen based on the balance of 0/1 bits in a bit vector
        // simply use a sequential algorithm to compute the remaining WT

        // compute sequential
        const size_t wsubtree_height = tlx::integer_log2_ceil(b-a+1);
        wt_pc<sym_t, idx_t>(
            bits,
            text,
            node_id,          // subtree root
            wsubtree_height); // subtree height

        // return
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
            if(size_t(text[i]) <= m) {
                bv[i] = 0;
                ++z;
            } else {
                bv[i] = 1;
            }
        }
    }

    if(a < m || m+1 < b) {
        // perform split
        const size_t split = dsplit_str(
            ctx,
            text,
            [m](const sym_t& x){return (size_t(x) > m);},
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

class mpi_dsplit {
public:

template<typename sym_t>
static void start(
    MPIContext& ctx,
    const std::string& input_filename,
    const size_t prefix,
    const size_t in_rdbufsize,
    const bool eff_input,
    const std::string& output) {

    Result::Time time;
    double t0 = ctx.time();

    auto dt = [&](){
        const double t = ctx.time();
        const double dt = t - t0;
        t0 = t;
        return dt;
    };

    // Determine input partition
    FilePartitionReader<sym_t> input(ctx, input_filename, prefix);
    const size_t local_num = input.local_num();
    const size_t rdbufsize = (in_rdbufsize > 0) ? in_rdbufsize : local_num;
    input.buffer(rdbufsize);
    
    time.input = dt();

    // Compute histogram
    ctx.cout_master() << "Compute histogram ..." << std::endl;
    Histogram<sym_t> hist(ctx, input, rdbufsize);

    time.hist = dt();

    // Compute effective alphabet
    EffectiveAlphabet<sym_t> ea(hist);

    // Transform text and cache in RAM
    ctx.cout_master() << "Compute effective transformation ..." << std::endl;
    std::vector<sym_t> etext(local_num);
    {
        size_t i = 0;
        ea.transform(input, [&](sym_t x){ etext[i++] = x; }, rdbufsize);
    }

    time.eff = dt();
    input.free();

    // recursive WT
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

    time.construct = dt();

    // Convert to level-wise representation
    WaveletMatrix wm = wt_nodes.merge_to_matrix(ctx, input, hist, true);
    time.merge = dt();

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WM to disk ..." << std::endl;

        if(ctx.rank() == 0) {
            hist.save(output + "." + WaveletMatrixBase::histogram_extension());
            wm.save_z(output +  + "." + WaveletMatrixBase::z_extension());
        }

        wm.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-wm-dsplit", ctx, input, wm.sigma(), time);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
}
};

int main(int argc, char** argv) {
    return mpi_launch<mpi_dsplit>(argc, argv);
}

