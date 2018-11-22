# Find FOXXL
include(FindPackageHandleStandardArgs)

set(THRILL_ROOT_DIR "" CACHE PATH "Folder contains Thrill")

find_path(FOXXLL_INCLUDE_DIR foxxll/defines.hpp PATHS ${THRILL_ROOT_DIR}/extlib/foxxll)
find_library(FOXXLL_LIBRARY foxxll PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/extlib/foxxll/foxxll)
find_package_handle_standard_args(FOXXL DEFAULT_MSG FOXXLL_INCLUDE_DIR FOXXLL_LIBRARY)

if(FOXXL_FOUND)
    get_filename_component(FOXXLL_LIBRARY_DIR ${FOXXLL_LIBRARY} DIRECTORY)
    set(FOXXL_INCLUDE_DIRS ${FOXXLL_INCLUDE_DIR} ${FOXXLL_LIBRARY_DIR}/..)
    set(FOXXL_LIBRARIES ${FOXXLL_LIBRARY})
endif()
