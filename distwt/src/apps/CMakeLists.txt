# Sort
add_executable(distwt-sort distwt_sort_main.cpp)
target_link_libraries(distwt-sort distwt ${THRILL_DEPENDENCIES})

# Recursive
add_executable(distwt-recursive distwt_recursive_main.cpp)
target_link_libraries(distwt-recursive distwt ${THRILL_DEPENDENCIES})

# Levelwise verifier
add_executable(distwt-verify distwt_verify_main.cpp)
target_link_libraries(distwt-verify distwt ${THRILL_DEPENDENCIES})
