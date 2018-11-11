#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/integer_log2.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>

void recursiveWT(
    const MPIContext& ctx,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer) {

    if(a == b) return;

    ctx.cout() << "recursiveWT(" <<
        "node_id=" << node_id <<
        ", n=" << n <<
        ", a=" << a <<
        ", b=" << b << std::endl;

    const size_t m = (a + b) / 2;

    // compute node bit vector
    size_t z = 0;
    {
        std::vector<bool> bv(n);
        for(size_t i = 0; i < n; i++) {
            if(text[i] <= m) {
                bv[i] = 0;
                ++z;
            } else {
                bv[i] = 1;
            }
        }

        // TODO: store?
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
            ctx,
            2ULL * node_id, // left child
            buffer, z,
            a, m,
            text);

        // recurse on right part
        recursiveWT(
            ctx,
            2ULL * node_id + 1ULL, // right child
            buffer + z, n - z,
            m+1, b,
            text + z);
    }
}

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    // Read command-line
    tlx::CmdlineParser cp;

    size_t memory = 512ULL * 1024ULL * 1024ULL; // default to 512 MiB
    cp.add_bytes('m', "mem", memory, "Amount of available local memory.");

    size_t rdbufsize = 64ULL * 1024ULL; // default to 64Ki
    cp.add_bytes('r', "rbuf", memory, "File read buffer size.");

    std::string input_filename; // reuquired
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Get input partition
    FilePartitionReader input(ctx, input_filename);

    // Compute histogram
    Histogram hist(ctx, input, rdbufsize);

    // FIXME: Right now, nothing is externalized, therefore we need some
    // minimum amount of memory to process texts.
    {
        const size_t wt_height = tlx::integer_log2_ceil(hist.size() - 1);
        size_t required_memory =
            // bit vector for root node
            input.local_num() / 8ULL +
            // 2x local effective transformation
            2ULL * input.local_num() * (sizeof(esym_t) / 8ULL);

        if(memory < required_memory) {
            ctx.cout(ctx.rank() == 0) <<
                "At least " << required_memory <<
                " bytes of memory is required!" <<
                " (wt_height = " << wt_height <<
                ", local_num = " << input.local_num() << ")" <<
                std::endl;
            return -2;
        }
    }

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    ctx.cout() << "Transform text" << std::endl;
    esym_t* etext = new esym_t[input.local_num()];
    {
        size_t i = 0;
        ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
    }

    // recursive WT
    esym_t* buffer = new esym_t[input.local_num()];
    recursiveWT(
        ctx,
        1ULL, // root
        etext, input.local_num(), // text
        0ULL, hist.size() - 1ULL, // interval
        buffer); // work buffer

    // Clean up and terminate
    delete[] buffer;
    delete[] etext;
    return 0;
}
