#pragma once

#include <mpi.h>

template<typename T> struct mpi_type;

template<> struct mpi_type<uint64_t> {
    static constexpr MPI_Datatype id() { return MPI_LONG_LONG; }
};

