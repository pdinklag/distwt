# Hello
add_executable(mpi-hello hello.cpp)
target_link_libraries(mpi-hello ${MPI_APP_DEPENDENCIES})

# Domain Decomposition
add_executable(mpi-dd dd.cpp)
target_link_libraries(mpi-dd ${MPI_APP_DEPENDENCIES})
