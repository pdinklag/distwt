add_library(distwt-mpi

    context.cpp
    malloc.cpp
    mpi_max.cpp
    mpi_sum.cpp
    mpi_type.cpp
    uint64_pack.cpp
    wm.cpp
    wt_levelwise.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
