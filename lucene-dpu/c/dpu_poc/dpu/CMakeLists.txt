cmake_minimum_required(VERSION 3.9)
project(dpu_poc C ASM)

include(DpuTools)

set(CMAKE_C_STANDARD 11)

file(GLOB SOURCES *.c)

dpu_build_bootstrap(__bootstrap.S SCRIPT config.kconf)
add_executable(dpu __bootstrap.S ${SOURCES})
set_target_properties(dpu PROPERTIES INTERPROCEDURAL_OPTIMIZATION True)
target_include_directories(dpu PUBLIC ../commons)
