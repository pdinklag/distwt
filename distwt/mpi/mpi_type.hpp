#pragma once

#include <mpi.h>
#include <distwt/mpi/uint_types.hpp>

template<typename T> struct mpi_type;

template<> struct mpi_type<uint8_t> {
    static constexpr MPI_Datatype id() { return MPI_BYTE; }
};

template<> struct mpi_type<uint16_t> {
    static constexpr MPI_Datatype id() { return MPI_SHORT; }
};

template<> struct mpi_type<uint32_t> {
    static constexpr MPI_Datatype id() { return MPI_INT; }
};

template<> struct mpi_type<uint40_t> {
private:
    static bool s_init;
    static MPI_Datatype s_mpi_type;

    static void init_type();

public:
    static MPI_Datatype id() {
        init_type();
        return s_mpi_type;
    }
};

template<> struct mpi_type<uint64_t> {
    static constexpr MPI_Datatype id() { return MPI_LONG_LONG; }
};
