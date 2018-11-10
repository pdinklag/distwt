#include <array>
#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

constexpr int TAG_LOCAL_HIST_ENTRY = 100;
constexpr int TAG_GLOBAL_HIST_ENTRY = 101;

constexpr size_t SIGMA_MAX = 256ULL;

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

    // Compute local histogram
    FilePartitionReader input(ctx, input_filename);

    size_t local_hist[SIGMA_MAX] = {0};
    input.process_local([&](unsigned char c){
        ++local_hist[c];
    }, memory);

    // distribute histogram
    size_t hist[SIGMA_MAX] = {0};
    MPI_Allreduce(local_hist, hist, SIGMA_MAX,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    // debug
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(local_hist[c] > 0) {
            ctx.cout() << "'" << (unsigned char)c << "' -> " << local_hist[c] << std::endl;
        }
    }

    return 0;
}
