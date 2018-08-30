set(THRILL_DEPENDENCIES
    ${THRILL_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${CMAKE_DL_LIBS})

add_executable(distwt

    distwt.cpp
    histogram.cpp)

target_link_libraries(distwt ${THRILL_DEPENDENCIES})
