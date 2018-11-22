# Sort
add_executable(thrill-sort sort.cpp)
target_link_libraries(thrill-sort ${THRILL_APP_DEPENDENCIES})

# Domain Decomposition
add_executable(thrill-dd dd.cpp)
target_link_libraries(thrill-dd ${THRILL_APP_DEPENDENCIES})

# Flat DD
add_executable(thrill-flat-dd flat_dd.cpp)
target_link_libraries(thrill-flat-dd ${THRILL_APP_DEPENDENCIES})

# Levelwise verifier
add_executable(verify-levelwise verify_levelwise.cpp)
target_link_libraries(verify-levelwise ${THRILL_APP_DEPENDENCIES})

# Node based verifier
#add_executable(verify-nodebased verify_nodebased.cpp)
#target_link_libraries(verify-nodebased ${THRILL_APP_DEPENDENCIES})
