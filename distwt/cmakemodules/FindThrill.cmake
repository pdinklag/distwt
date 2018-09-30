# Find Thrill and TLX
include(FindPackageHandleStandardArgs)

set(THRILL_ROOT_DIR "" CACHE PATH "Folder contains Thrill")

# Find TLX (in extlib)
find_path(TLX_INCLUDE_DIR tlx/define.hpp PATHS ${THRILL_ROOT_DIR}/extlib/tlx)
find_library(TLX_LIBRARY tlx PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/extlib/tlx/tlx)
find_package_handle_standard_args(Tlx DEFAULT_MSG TLX_INCLUDE_DIR TLX_LIBRARY)

# Find FOXXLL (in extlib)
find_path(FOXXLL_INCLUDE_DIR foxxll/defines.hpp PATHS ${THRILL_ROOT_DIR}/extlib/foxxll)
find_library(FOXXLL_LIBRARY foxxll PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/extlib/foxxll/foxxll)
find_package_handle_standard_args(Foxxll DEFAULT_MSG FOXXLL_INCLUDE_DIR FOXXLL_LIBRARY)

# Find Thrill
find_path(THRILL_INCLUDE_DIR thrill/api/dia.hpp PATHS ${THRILL_ROOT_DIR})
find_library(THRILL_LIBRARY thrill PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/thrill)
find_package_handle_standard_args(Thrill DEFAULT_MSG THRILL_INCLUDE_DIR THRILL_LIBRARY)

if(TLX_FOUND AND FOXXLL_FOUND AND THRILL_FOUND)
    get_filename_component(FOXXLL_LIBRARY_DIR ${FOXXLL_LIBRARY} DIRECTORY)
    set(THRILL_INCLUDE_DIRS ${THRILL_INCLUDE_DIR} ${FOXXLL_INCLUDE_DIR} ${FOXXLL_LIBRARY_DIR}/.. ${TLX_INCLUDE_DIR})
    set(THRILL_LIBRARIES ${THRILL_LIBRARY} ${FOXXLL_LIBRARY} ${TLX_LIBRARY})
endif()

# Optionally find s3 (in extlib)
find_path(S3_INCLUDE_DIR libs3/inc/libs3.h PATHS ${THRILL_ROOT_DIR}/extlib/libs3)
find_library(S3_LIBRARY s3 PATHS ${THRILL_ROOT_DIR} PATH_SUFFIXES build/extlib/libs3)
find_package_handle_standard_args(S3 DEFAULT_MSG S3_INCLUDE_DIR S3_LIBRARY)

if(S3_FOUND)
    # s3 available, make sure to find dependencies too
    find_package(CURL REQUIRED)
    find_package(LibXml2 REQUIRED)
    find_package(OpenSSL REQUIRED)

    set(THRILL_LIBRARIES ${THRILL_LIBRARIES} ${S3_LIBRARY} ${CURL_LIBRARIES} ${LIBXML2_LIBRARIES} ${OPENSSL_LIBRARIES})
endif()

# Optionally find MPI
find_package(MPI)

if(MPI_FOUND)
    set(THRILL_LIBRARIES ${THRILL_LIBRARIES} ${MPI_LIBRARIES})
endif()

# Optionally find Intel TBB
find_package(TBB)

if(TBB_FOUND)
    set(THRILL_LIBRARIES ${THRILL_LIBRARIES} ${TBB_LIBRARIES})
endif()

