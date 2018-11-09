# Sort
add_executable(sort sort.cpp)
target_link_libraries(sort ${THRILL_APP_DEPENDENCIES})

# Recursive
add_executable(recursive recursive.cpp)
target_link_libraries(recursive ${THRILL_APP_DEPENDENCIES})

# Flat
add_executable(flat flat.cpp)
target_link_libraries(flat ${THRILL_APP_DEPENDENCIES})

# Levelwise verifier
add_executable(verify-levelwise verify_levelwise.cpp)
target_link_libraries(verify-levelwise ${THRILL_APP_DEPENDENCIES})

# Node based verifier
add_executable(verify-nodebased verify_nodebased.cpp)
target_link_libraries(verify-nodebased ${THRILL_APP_DEPENDENCIES})
