#include <distwt/mpi/mpi_type.hpp>

bool mpi_type<uint40_t>::s_init = false;
MPI_Datatype mpi_type<uint40_t>::s_mpi_type;

void mpi_type<uint40_t>::init_type() {
    if(!s_init) {
        MPI_Type_contiguous(5, MPI_BYTE, &s_mpi_type);
        MPI_Type_commit(&s_mpi_type);
        s_init = true;
    }
}
