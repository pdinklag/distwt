add_library(distwt-thrill

    effective_alphabet.cpp
    histogram.cpp
    wt.cpp
)
target_link_libraries(distwt-thrill ${THRILL_DEPENDENCIES})
