# Find Thrill and TLX
include(FindPackageHandleStandardArgs)

set(THRILL_ROOT_DIR "" CACHE PATH "Folder contains Thrill")

# Find TLX (in extlib)
find_path(TLX_INCLUDE_DIR tlx/define.hpp PATHS ${THRILL_ROOT_DIR}/extlib/tlx)

find_library(TLX_LIBRARY tlx
    PATHS ${THRILL_ROOT_DIR}
    PATH_SUFFIXES build/extlib/tlx/tlx)

find_package_handle_standard_args(Tlx DEFAULT_MSG TLX_INCLUDE_DIR TLX_LIBRARY)

# Find FOXXLL (in extlib)
find_path(FOXXLL_INCLUDE_DIR foxxll/defines.hpp PATHS ${THRILL_ROOT_DIR}/extlib/foxxll)

find_library(FOXXLL_LIBRARY foxxll
    PATHS ${THRILL_ROOT_DIR}
    PATH_SUFFIXES build/extlib/foxxll/foxxll)

find_package_handle_standard_args(Foxxll DEFAULT_MSG FOXXLL_INCLUDE_DIR FOXXLL_LIBRARY)

# Find Thrill
find_path(THRILL_INCLUDE_DIR thrill/api/dia.hpp PATHS ${THRILL_ROOT_DIR})

find_library(THRILL_LIBRARY thrill
    PATHS ${THRILL_ROOT_DIR}
    PATH_SUFFIXES build/thrill)

find_package_handle_standard_args(Thrill DEFAULT_MSG
    THRILL_INCLUDE_DIR THRILL_LIBRARY)

if(TLX_FOUND AND FOXXLL_FOUND AND THRILL_FOUND)
    get_filename_component(FOXXLL_LIBRARY_DIR ${FOXXLL_LIBRARY} DIRECTORY)
    set(THRILL_INCLUDE_DIRS ${THRILL_INCLUDE_DIR} ${FOXXLL_INCLUDE_DIR} ${FOXXLL_LIBRARY_DIR}/.. ${TLX_INCLUDE_DIR})
    set(THRILL_LIBRARIES ${THRILL_LIBRARY} ${FOXXLL_LIBRARY} ${TLX_LIBRARY})
endif()

