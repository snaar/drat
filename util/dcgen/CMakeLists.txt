cmake_minimum_required(VERSION 2.8)

project(dcgen)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2 -Wall -Wno-unused-function -pedantic")

set(SOURCE_FILES dcgen.cpp)
add_executable(dcgen ${SOURCE_FILES})
