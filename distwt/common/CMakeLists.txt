add_library(distwt-common

    bitrev.cpp
    effective_alphabet.cpp
    histogram.cpp
    result.cpp
    wm.cpp
    wm_sequential.cpp
    wt.cpp
    wt_sequential.cpp
)
target_link_libraries(distwt-common ${COMMON_DEPENDENCIES})
