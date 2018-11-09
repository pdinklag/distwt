# Hello
add_executable(hello hello.cpp)
target_link_libraries(hello ${MPI_APP_DEPENDENCIES})

# Test
add_executable(mpihist hist.cpp)
target_link_libraries(mpihist ${MPI_APP_DEPENDENCIES})
