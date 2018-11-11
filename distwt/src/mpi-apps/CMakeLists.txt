# Histogram computation
add_executable(mpi-hist hist.cpp)
target_link_libraries(mpi-hist ${MPI_APP_DEPENDENCIES})

# recursiveWT
add_executable(mpi-recursive recursive.cpp)
target_link_libraries(mpi-recursive ${MPI_APP_DEPENDENCIES})
