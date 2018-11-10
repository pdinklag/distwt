#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

constexpr size_t SIGMA_MAX = 256ULL;

void compute_histogram(
    const FilePartitionReader& input,
    const size_t bufsize,
    size_t* hist) {

    // Compute local histogram
    size_t local_hist[SIGMA_MAX] = {0};
    input.process_local([&](unsigned char c){
        ++local_hist[c];
    }, bufsize);

    // distribute
    MPI_Allreduce(local_hist, hist, SIGMA_MAX,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    // Read command-line
    tlx::CmdlineParser cp;

    size_t memory = 268'435'456ULL; // default to 256 MiB
    cp.add_bytes('m', "mem", memory, "Amount of available local memory.");

    std::string input_filename; // reuquired
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Get input partition
    FilePartitionReader input(ctx, input_filename);

    // Compute histogram
    size_t hist[SIGMA_MAX] = {0};
    compute_histogram(input, memory, hist);

    // debug
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(hist[c] > 0) {
            ctx.cout() << "'" << (unsigned char)c << "' -> " << hist[c] << std::endl;
        }
    }

    return 0;
}
