#include <mpi.h>
#include <stdio.h>

int main(int argc, char** argv) {
    int num, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &num);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    printf("Hello from processor #%d out of %d\n", rank, num);

    MPI_Finalize();
}

