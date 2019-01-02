add_library(distwt-common

    effective_alphabet.cpp
    histogram.cpp
    result.cpp
    wt.cpp
    wt_sequential.cpp
)
target_link_libraries(distwt-common ${COMMON_DEPENDENCIES})
