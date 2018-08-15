set(THRILL_DEPENDENCIES
    ${THRILL_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${CMAKE_DL_LIBS})

add_executable(distwt distwt.cpp)
target_link_libraries(distwt ${THRILL_DEPENDENCIES})

add_executable(stable_sort stable_sort.cpp)
target_link_libraries(stable_sort ${THRILL_DEPENDENCIES})
