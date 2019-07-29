#pragma once

#include <mpi.h>
#include <distwt/mpi/uint_types.hpp>

template<typename T> struct mpi_max;

template<> struct mpi_max<uint8_t> {
    static constexpr MPI_Op op() { return MPI_MAX; }
};

template<> struct mpi_max<uint16_t> {
    static constexpr MPI_Op op() { return MPI_MAX; }
};

template<> struct mpi_max<uint32_t> {
    static constexpr MPI_Op op() { return MPI_MAX; }
};

template<> struct mpi_max<uint40_t> {
private:
    static bool s_init;
    static MPI_Op s_mpi_op;

    static void init_op();

public:
    static MPI_Op op() {
        init_op();
        return s_mpi_op;
    }
};

template<> struct mpi_max<uint64_t> {
    static constexpr MPI_Op op() { return MPI_MAX; }
};

