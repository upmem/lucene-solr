cmake_minimum_required(VERSION 3.13)
project(x86_poc C)

set(CMAKE_C_STANDARD 11)

add_executable(x86_poc main.c data_input.c data_input.h fst.c fst.h segment_terms_enum.c segment_terms_enum.h bytes_ref.c bytes_ref.h)