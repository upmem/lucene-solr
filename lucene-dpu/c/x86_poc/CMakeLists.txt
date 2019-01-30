cmake_minimum_required(VERSION 3.6)
project(x86_poc C)

set(CMAKE_C_STANDARD 11)

file(GLOB SOURCES *.c)

add_executable(x86_poc ${SOURCES})

target_link_libraries(x86_poc m)