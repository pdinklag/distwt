# Sort
add_executable(distwt-sort distwt_sort_main.cpp)
target_link_libraries(distwt-sort distwt ${THRILL_DEPENDENCIES})

# Recursive
add_executable(distwt-recursive distwt_recursive_main.cpp)
target_link_libraries(distwt-recursive distwt ${THRILL_DEPENDENCIES})

# Flat
add_executable(distwt-flat distwt_flat_main.cpp)
target_link_libraries(distwt-flat distwt ${THRILL_DEPENDENCIES})

# Reduce
add_executable(distwt-reduce distwt_reduce_main.cpp)
target_link_libraries(distwt-reduce distwt ${THRILL_DEPENDENCIES})

# Levelwise verifier
add_executable(distwt-verify distwt_verify_main.cpp)
target_link_libraries(distwt-verify distwt ${THRILL_DEPENDENCIES})
