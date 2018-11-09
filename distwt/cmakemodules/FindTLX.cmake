# Find TLX
include(FindPackageHandleStandardArgs)

set(THRILL_ROOT_DIR "" CACHE PATH "Folder contains Thrill")

find_path(TLX_INCLUDE_DIR tlx/define.hpp PATHS ${THRILL_ROOT_DIR}/extlib/tlx)
find_library(TLX_LIBRARY tlx PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/extlib/tlx/tlx)
find_package_handle_standard_args(TLX DEFAULT_MSG TLX_INCLUDE_DIR TLX_LIBRARY)

if(TLX_FOUND)
    set(TLX_INCLUDE_DIRS ${TLX_INCLUDE_DIR})
    set(TLX_LIBRARIES ${TLX_LIBRARY})
    include_directories(${TLX_INCLUDE_DIRS})
endif()
