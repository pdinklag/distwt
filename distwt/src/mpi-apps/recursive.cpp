#include <vector>

#include <tlx/cmdline_parser.hpp>

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

    size_t rdbufsize = 64ULL * 1024ULL; // default to 64Ki
    cp.add_bytes('r', "rbuf", rdbufsize, "File read buffer size.");

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Init MPI
    MPIContext ctx(&argc, &argv);
    ctx.cout(ctx.rank() == 0) <<
        "Starting computation with " << ctx.num_workers() << " workers ..." << std::endl;

    // Get input partition
    FilePartitionReader input(ctx, input_filename);

    // Compute histogram
    ctx.cout(ctx.rank() == 0) << "Compute histogram ..." << std::endl;
    Histogram hist(ctx, input, rdbufsize);

    // prepare WT
    WaveletTreeBase wt(hist);
    bits_t bits(wt.num_nodes());

    // FIXME: Right now, nothing is externalized, therefore we need some
    // minimum amount of memory to process texts.
    {
        const size_t required_memory =
            // bit vectors
            (wt.height() * input.local_num()) / 8ULL +
            // 2x local effective transformation
            2ULL * input.local_num() * (sizeof(esym_t) / 8ULL);

        ctx.cout(ctx.rank() == 0) << "required_memory: " <<
            required_memory << " bytes" << std::endl;
    }

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    {
        ctx.cout(ctx.rank() == 0) << "Compute effective transformation ..." << std::endl;
        esym_t* etext = new esym_t[input.local_num()];
        {
            size_t i = 0;
            ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
        }

        // recursive WT
        ctx.cout(ctx.rank() == 0) << "Compute WT ..." << std::endl;
        esym_t* buffer = new esym_t[input.local_num()];
        recursiveWT(
            bits,
            ctx,
            1ULL, // root
            etext, input.local_num(), // text
            0ULL, wt.num_nodes(), // interval
            buffer); // work buffer

        // Clean up
        delete[] buffer;
        delete[] etext;
    }

    // Finalize
    ctx.cout(ctx.rank() == 0) << "Done computing " << bits.size() << " nodes." << std::endl;
    return 0;
}
