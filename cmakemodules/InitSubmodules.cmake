# find Git
find_package(Git)

if(GIT_FOUND)
    # update submodules
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" "submodule" "update" "--init" "--recursive"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        RESULT_VARIABLE GIT_RESULT)

    if(NOT ${GIT_RESULT} EQUAL 0)
        message(WARNING "Failed to initialize submodules: ${GIT_RESULT}")
    endif()
else()
    message(WARNING "Could not find git -- submodules must be initialized manually")
endif()

