set(THRILL_DEPENDENCIES
    ${THRILL_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${CMAKE_DL_LIBS})

add_library(distwt

    histogram.cpp
    effective_alphabet.cpp
    wt.cpp
)
target_link_libraries(distwt ${THRILL_DEPENDENCIES})

add_subdirectory(apps)
add_subdirectory(mpi)

