# distwt-common
set(COMMON_DEPENDENCIES ${TLX_LIBRARIES})
add_subdirectory(common)

# distwt-mpi
set(MPI_DEPENDENCIES
    distwt-common
    ${MPI_LIBRARIES}
    ${TLX_LIBRARIES})
add_subdirectory(mpi)

# mpi apps
set(MPI_APP_DEPENDENCIES distwt-mpi ${MPI_DEPENDENCIES})
add_subdirectory(mpi-apps)

# distwt-thrill
set(THRILL_DEPENDENCIES
    distwt-common
    ${THRILL_LIBRARIES}
    ${FOXXL_LIBRARIES}
    ${TLX_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ${MPI_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${CMAKE_DL_LIBS})
add_subdirectory(thrill)

# Thrill apps
set(THRILL_APP_DEPENDENCIES distwt-thrill ${THRILL_DEPENDENCIES})
add_subdirectory(thrill-apps)
