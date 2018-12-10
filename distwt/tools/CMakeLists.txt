# Alloc tester
add_executable(alloc-test alloc_test.cpp)
target_link_libraries(alloc-test ${TLX_LIBRARIES})

# Histogram computation
add_executable(histogram histogram.cpp)
target_link_libraries(histogram ${TLX_LIBRARIES})

# Text processor
add_executable(process process.cpp)
target_link_libraries(process ${TLX_LIBRARIES})
