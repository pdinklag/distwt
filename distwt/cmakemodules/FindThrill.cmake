# Find Thrill and TLX
include(FindPackageHandleStandardArgs)

set(THRILL_ROOT_DIR "" CACHE PATH "Folder contains Thrill")

# Find TLX (in extlib)
find_path(TLX_INCLUDE_DIR tlx/define.hpp PATHS ${THRILL_ROOT_DIR}/extlib/tlx)

find_library(TLX_LIBRARY tlx
    PATHS ${THRILL_ROOT_DIR}
    PATH_SUFFIXES build/extlib/tlx/tlx)

find_package_handle_standard_args(Tlx DEFAULT_MSG TLX_INCLUDE_DIR TLX_LIBRARY)

# Find Thrill
find_path(THRILL_INCLUDE_DIR thrill/api/dia.hpp PATHS ${THRILL_ROOT_DIR})

find_library(THRILL_LIBRARY thrill
    PATHS ${THRILL_ROOT_DIR}
    PATH_SUFFIXES build/thrill)

find_package_handle_standard_args(Thrill DEFAULT_MSG
    THRILL_INCLUDE_DIR THRILL_LIBRARY)

if(TLX_FOUND AND THRILL_FOUND)
    set(THRILL_INCLUDE_DIRS ${THRILL_INCLUDE_DIR} ${TLX_INCLUDE_DIR})
    set(THRILL_LIBRARIES ${THRILL_LIBRARY} ${TLX_LIBRARY})
endif()

