cmake_minimum_required(VERSION 3.8.2 FATAL_ERROR)
project(distwt)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmakemodules")

set(CXX_STANDARD c++14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=gnu++14 ${GCC_WARNINGS}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb")

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "CMAKE_BUILD_TYPE not defined - setting to Release.")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    message(STATUS "CMAKE_BUILD_TYPE is Release")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    message(STATUS "CMAKE_BUILD_TYPE is Debug")
else()
    message(FATAL_ERROR "Unsupported CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
endif()

# init submodules
include(InitSubmodules)

# tlx
set(TDC_EXTLIB_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/extlib")
add_subdirectory(${TDC_EXTLIB_SOURCE_DIR}/tlx)
include_directories(${TDC_EXTLIB_SOURCE_DIR}/tlx)

# required
find_package(Threads REQUIRED)
find_package(MPI REQUIRED)

# Thrill related (optional)
find_package(ZLIB)
find_package(BZip2)
find_package(FOXXL)
find_package(Thrill)

include_directories(${MPI_INCLUDE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if(THRILL_FOUND)
    include_directories(${FOXXL_INCLUDE_DIRS})
    include_directories(${THRILL_INCLUDE_DIRS})
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_subdirectory(distwt)
