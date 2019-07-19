#pragma once

#include <mpi.h>

template<typename T> struct mpi_type;

template<> struct mpi_type<unsigned char> {
    static constexpr MPI_Datatype id() { return MPI_BYTE; }
};

template<> struct mpi_type<unsigned short> {
    static constexpr MPI_Datatype id() { return MPI_SHORT; }
};

template<> struct mpi_type<unsigned int> {
    static constexpr MPI_Datatype id() { return MPI_INT; }
};

template<> struct mpi_type<uint64_t> {
    static constexpr MPI_Datatype id() { return MPI_LONG_LONG; }
};
