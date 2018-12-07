# MPI Domain Decomposition
add_executable(mpi-dd mpi_dd.cpp)
target_link_libraries(mpi-dd ${MPI_APP_DEPENDENCIES})

# MPI Parallel Split
add_executable(mpi-parsplit mpi_parsplit.cpp)
target_link_libraries(mpi-parsplit ${MPI_APP_DEPENDENCIES})

# Thrill Sort
add_executable(thrill-sort thrill_sort.cpp)
target_link_libraries(thrill-sort ${THRILL_APP_DEPENDENCIES})

# Thrill Domain Decomposition
add_executable(thrill-dd thrill_dd.cpp)
target_link_libraries(thrill-dd ${THRILL_APP_DEPENDENCIES})

# Thrill Flat DD
add_executable(thrill-flat-dd thrill_flat_dd.cpp)
target_link_libraries(thrill-flat-dd ${THRILL_APP_DEPENDENCIES})

# Thrill Levelwise verifier
add_executable(verify-levelwise verify_levelwise.cpp)
target_link_libraries(verify-levelwise ${THRILL_APP_DEPENDENCIES})
