add_library(distwt-mpi

    context.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
