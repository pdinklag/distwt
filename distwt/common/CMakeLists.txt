add_library(distwt-common

    bitrev.cpp
    result.cpp
    wm.cpp
)
target_link_libraries(distwt-common ${COMMON_DEPENDENCIES})
