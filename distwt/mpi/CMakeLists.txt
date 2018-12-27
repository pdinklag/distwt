add_library(distwt-mpi

    context.cpp
    effective_alphabet.cpp
    file_partition_reader.cpp
    histogram.cpp
    malloc.cpp
    result.cpp
    wt_levelwise.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
