#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    // Read command-line
    tlx::CmdlineParser cp;

    size_t memory = 268'435'456ULL; // default to 256 MiB
    cp.add_bytes('m', "mem", memory, "Amount of available local memory.");

    std::string input_filename; // reuquired
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv, ctx.cout(ctx.rank() == 0))) {
        return -1;
    }

    // Count local histogram
    FilePartitionReader input(ctx, input_filename);

    ctx.cout() << "Process partial text: ";
    input.process_local([&](unsigned char c){
        std::cout << c;
    }, memory);
    std::cout << std::endl;
    return 0;
}
