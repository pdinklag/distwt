# Histogram computation
add_executable(mpi-hist hist.cpp)
target_link_libraries(mpi-hist ${MPI_APP_DEPENDENCIES})
