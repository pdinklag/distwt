#include <distwt/mpi/mpi_sum.hpp>

bool mpi_sum<uint40_t>::s_init = false;
MPI_Op mpi_sum<uint40_t>::s_mpi_op;

void mpi_sum_uint40(void* _a, void* _b, int* len, MPI_Datatype* type) {
    uint40_t* a = (uint40_t*)_a;
    uint40_t* b = (uint40_t*)_b;
    for(int i = 0; i < *len; i++) {
        b[i] = a[i] + b[i];
    }
}

void mpi_sum<uint40_t>::init_op() {
    if(!s_init) {
        MPI_Op_create(mpi_sum_uint40, 1, &s_mpi_op);
        s_init = true;
    }
}
