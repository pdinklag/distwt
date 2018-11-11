#include <distwt/mpi/context.hpp>

int main(int argc, char** argv) {
    MPIContext ctx(&argc, &argv);
    ctx.cout() << "Hello!" << std::endl;
}
