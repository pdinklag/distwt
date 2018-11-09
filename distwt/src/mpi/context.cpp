#include <distwt/mpi/context.hpp>

MPIContext::MPIContext(int* argc, char*** argv) {
    MPI_Init(argc, argv);
    MPI_Comm_size(MPI_COMM_WORLD, &m_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &m_rank);
}

MPIContext::~MPIContext() {
    MPI_Finalize();
}
