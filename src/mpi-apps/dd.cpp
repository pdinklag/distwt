#include <bitset>
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
#include <distwt/mpi/wt_nodebased.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

#include <distwt/mpi/result.hpp>

void recursiveWT(
    WaveletTree::bits_t& bits,
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

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    ctx.cout_master() << "Compute effective transformation ..." << std::endl;
    esym_t* etext = new esym_t[local_num];
    {
        size_t i = 0;
        ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
    }

    // recursive WT
    ctx.cout_master() << "Compute WT ..." << std::endl;
    auto wt_nodes = WaveletTreeNodebased(hist,
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        bits.resize(wt.num_nodes());

        esym_t* buffer = new esym_t[local_num];
        recursiveWT(
            bits,
            ctx,
            1ULL, // root
            etext, local_num, // text
            0ULL, wt.num_nodes(), // interval
            buffer); // work buffer

        delete[] buffer;
    });

    // Clean up
    delete[] etext;

    // Synchronize
    ctx.cout_master() << "Done computing " << wt_nodes.num_nodes()
        << " nodes. Synchronizing ..." << std::endl;
    ctx.synchronize();

    // Convert to level-wise representation
    WaveletTreeLevelwise wt = wt_nodes.merge(ctx, input, hist, true);

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
    const double dt = util::time() - t0;
    Result result("mpi-dd", ctx, input, dt);

    ctx.cout_master() << result.sqlplot() << std::endl;
    ctx.cout_master() << result.readable() << std::endl;
    return 0;
}
