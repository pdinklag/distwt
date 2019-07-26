add_library(distwt-mpi

    context.cpp
    malloc.cpp
    uint64_pack.cpp
    wm.cpp
    wt_levelwise.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
