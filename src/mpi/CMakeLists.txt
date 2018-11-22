add_library(distwt-mpi

    bit_vector.cpp
    context.cpp
    effective_alphabet.cpp
    histogram.cpp
    file_partition_reader.cpp
)
target_link_libraries(distwt-mpi ${MPI_DEPENDENCIES})
