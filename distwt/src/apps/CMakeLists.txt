# Sort
add_executable(sort sort.cpp)
target_link_libraries(sort distwt ${THRILL_DEPENDENCIES})

# Recursive
add_executable(recursive recursive.cpp)
target_link_libraries(recursive distwt ${THRILL_DEPENDENCIES})

# Flat
add_executable(flat flat.cpp)
target_link_libraries(flat distwt ${THRILL_DEPENDENCIES})

# Levelwise verifier
add_executable(verify-levelwise verify_levelwise.cpp)
target_link_libraries(verify-levelwise distwt ${THRILL_DEPENDENCIES})

# Node based verifier
add_executable(verify-nodebased verify_nodebased.cpp)
target_link_libraries(verify-nodebased distwt ${THRILL_DEPENDENCIES})
