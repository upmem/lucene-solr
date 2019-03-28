cmake_minimum_required(VERSION 3.9)
project(dpu_poc C ASM)

set(CMAKE_C_STANDARD 11)

file(GLOB SOURCES *.c)

add_executable(dpu ${SOURCES})
set_target_properties(dpu PROPERTIES INTERPROCEDURAL_OPTIMIZATION True)
target_include_directories(dpu PUBLIC ../commons)
