add_library(distwt-mpi

    bit_vector.cpp
    context.cpp
    effective_alphabet.cpp
    file_partition_reader.cpp
    histogram.cpp
    result.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
