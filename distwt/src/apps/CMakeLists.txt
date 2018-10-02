# Implementation of the baseline algorithm
add_executable(distwt-baseline
    distwt_baseline_main.cpp
)
target_link_libraries(distwt-baseline distwt ${THRILL_DEPENDENCIES})

# WT Verifier
add_executable(distwt-verify
    distwt_verify_main.cpp
)
target_link_libraries(distwt-verify distwt ${THRILL_DEPENDENCIES})
