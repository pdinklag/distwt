add_library(distwt-mpi

    context.cpp
    file_partition_reader.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
