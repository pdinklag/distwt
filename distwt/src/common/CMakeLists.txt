add_library(distwt-common

    effective_alphabet.cpp
    histogram.cpp
    wt.cpp
)
target_link_libraries(distwt-common ${COMMON_DEPENDENCIES})
