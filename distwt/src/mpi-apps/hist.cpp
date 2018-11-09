#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    // Read command-line
    if(argc < 2) {
        ctx.cout(ctx.rank() == 0) << "Usage: "
            << argv[0] << " FILE" << std::endl;

        return -1;
    }

    FilePartitionReader input(ctx, argv[1]);

    ctx.cout() << "Process partial text: ";
    input.process_local([&](unsigned char c){
        std::cout << c;
    }, 4194304ULL /* 4 MiB */);
    std::cout << std::endl;
    return 0;
}
