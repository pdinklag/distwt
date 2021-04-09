# Tools
add_subdirectory(tools)

# distwt-common
set(COMMON_DEPENDENCIES tlx)
add_subdirectory(common)

# distwt-mpi
set(MPI_DEPENDENCIES distwt-common ${MPI_LIBRARIES} tlx)
add_subdirectory(mpi)

# distwt-thrill
if(THRILL_FOUND)
    set(THRILL_DEPENDENCIES
        distwt-common
        ${THRILL_LIBRARIES}
        ${FOXXL_LIBRARIES}
        tlx
        ${ZLIB_LIBRARIES}
        ${BZIP2_LIBRARIES}
        ${MPI_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        ${CMAKE_DL_LIBS})
    add_subdirectory(thrill)
endif()

# Apps
set(THRILL_APP_DEPENDENCIES distwt-thrill ${THRILL_DEPENDENCIES})
set(MPI_APP_DEPENDENCIES distwt-mpi ${MPI_DEPENDENCIES})
add_subdirectory(apps)
