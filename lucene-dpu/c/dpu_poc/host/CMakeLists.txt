cmake_minimum_required(VERSION 3.6)
project(dpuhost_poc C)

set(CMAKE_C_STANDARD 11)

file(GLOB SOURCES *.c)

add_executable(dpuhost_poc ${SOURCES})