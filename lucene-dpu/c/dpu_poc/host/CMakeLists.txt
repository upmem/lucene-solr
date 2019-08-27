cmake_minimum_required(VERSION 3.6)
project(dpuhost_poc C)

include(ExternalProject)

set(CMAKE_C_STANDARD 11)

if (NOT DEFINED UPMEM_HOME)
    set(UPMEM_HOME $ENV{UPMEM_HOME})
endif ()

include(${UPMEM_HOME}/usr/share/upmem/cmake/include/host/DpuHost.cmake)

file(GLOB SOURCES *.c x86_process/*.c)

set(DPU_PROJECT_RELATIVE_PATH ../dpu)
set(DPU_BINARY ${CMAKE_CURRENT_BINARY_DIR}/${DPU_PROJECT_RELATIVE_PATH}/dpu)

set(DPU_HOST_LIBRARIES dpu)

add_executable(dpuhost_poc ${SOURCES})
target_compile_definitions(dpuhost_poc PUBLIC DPU_BINARY=${DPU_BINARY})
target_link_libraries(dpuhost_poc ${DPU_HOST_LIBRARIES} m)
target_include_directories(dpuhost_poc SYSTEM PUBLIC ${DPU_HOST_INCLUDE_DIRECTORIES})
target_link_directories(dpuhost_poc PUBLIC ${DPU_HOST_LINK_DIRECTORIES})
target_include_directories(dpuhost_poc PUBLIC ../commons)

###############
# DPU Project #
###############
set(DPU_PROJECT_BUILD_TYPE MinSizeRel)

ExternalProject_Add(
        dpu
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/${DPU_PROJECT_RELATIVE_PATH}
        BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/${DPU_PROJECT_RELATIVE_PATH}
        CMAKE_ARGS -DCMAKE_TOOLCHAIN_FILE=${UPMEM_HOME}/usr/share/upmem/cmake/dpu.cmake -DCMAKE_BUILD_TYPE=${DPU_PROJECT_BUILD_TYPE} -DUPMEM_HOME=${UPMEM_HOME}
        BUILD_ALWAYS TRUE
        INSTALL_COMMAND ""
)

add_dependencies(dpuhost_poc dpu)
