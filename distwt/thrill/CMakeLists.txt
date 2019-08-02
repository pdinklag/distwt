add_library(distwt-thrill

    effective_alphabet.cpp
    histogram.cpp
    result.cpp
    wm.cpp
    wt_levelwise.cpp
    wt_nodebased.cpp
)
target_link_libraries(distwt-thrill ${THRILL_DEPENDENCIES})
