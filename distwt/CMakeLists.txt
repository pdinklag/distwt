cmake_minimum_required(VERSION 3.8.2 FATAL_ERROR)
project(distwt)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmakemodules") 

set(CXX_STANDARD c++14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=gnu++14 ${GCC_WARNINGS}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -DDEBUG")

find_package(Threads)
find_package(ZLIB)
find_package(BZip2)
find_package(Thrill)

include_directories(${TLX_INCLUDE_DIRS})
include_directories(${THRILL_INCLUDE_DIRS})

add_subdirectory(src)

