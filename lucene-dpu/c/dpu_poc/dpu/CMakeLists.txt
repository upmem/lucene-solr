cmake_minimum_required(VERSION 3.6)
project(dpu_poc C ASM)

include(DpuTools)

set(CMAKE_C_STANDARD 11)

file(GLOB SOURCES *.c)

dpu_build_bootstrap(__bootstrap.s SCRIPT config.kconf)
add_executable(dpu __bootstrap.s ${SOURCES})