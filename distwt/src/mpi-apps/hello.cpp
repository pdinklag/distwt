#include <distwt/mpi/context.hpp>

int main(int argc, char** argv) {
    MPIContext ctx(&argc, &argv);
    printf("Hello from processor #%d out of %d\n",
        ctx.rank(), ctx.num_workers());
}

