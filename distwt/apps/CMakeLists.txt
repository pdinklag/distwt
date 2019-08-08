# MPI Domain Decomposition
add_executable(mpi-dd mpi_dd.cpp)
target_link_libraries(mpi-dd ${MPI_APP_DEPENDENCIES})

# MPI Distributed Split
add_executable(mpi-dsplit mpi_dsplit.cpp)
target_link_libraries(mpi-dsplit ${MPI_APP_DEPENDENCIES})

# MPI Bucket Sort
add_executable(mpi-bsort mpi_bsort.cpp)
target_link_libraries(mpi-bsort ${MPI_APP_DEPENDENCIES})

# MPI Bucket Sort with dynamic bucket allocation
add_executable(mpi-dynbsort mpi_dynbsort.cpp)
target_link_libraries(mpi-dynbsort ${MPI_APP_DEPENDENCIES})

# MPI WM Concat
add_executable(mpi-wm-concat mpi_wm_concat.cpp)
target_link_libraries(mpi-wm-concat ${MPI_APP_DEPENDENCIES})

# MPI WM Domain Decomposition
add_executable(mpi-wm-dd mpi_wm_dd.cpp)
target_link_libraries(mpi-wm-dd ${MPI_APP_DEPENDENCIES})

# MPI WM Split
add_executable(mpi-wm-dsplit mpi_wm_dsplit.cpp)
target_link_libraries(mpi-wm-dsplit ${MPI_APP_DEPENDENCIES})

# Thrill Sort
add_executable(thrill-sort thrill_sort.cpp)
target_link_libraries(thrill-sort ${THRILL_APP_DEPENDENCIES})

# Thrill Bucket Sort
add_executable(thrill-bsort thrill_bsort.cpp)
target_link_libraries(thrill-bsort ${THRILL_APP_DEPENDENCIES})

# Thrill Domain Decomposition
add_executable(thrill-dd thrill_dd.cpp)
target_link_libraries(thrill-dd ${THRILL_APP_DEPENDENCIES})

# Thrill WM Concat
add_executable(thrill-wm-concat thrill_wm_concat.cpp)
target_link_libraries(thrill-wm-concat ${THRILL_APP_DEPENDENCIES})

# Thrill Input Benchmark
add_executable(thrill-input thrill_input.cpp)
target_link_libraries(thrill-input ${THRILL_APP_DEPENDENCIES})

# Thrill Levelwise verifier
add_executable(verify-levelwise verify_levelwise.cpp)
target_link_libraries(verify-levelwise ${THRILL_APP_DEPENDENCIES})
