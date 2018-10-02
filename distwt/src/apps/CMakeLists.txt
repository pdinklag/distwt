add_executable(distwt-test
    distwt_test_main.cpp
)
target_link_libraries(distwt-test distwt ${THRILL_DEPENDENCIES})
