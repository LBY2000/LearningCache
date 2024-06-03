cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(Sherman)

# 设置Boost的路径
set(BOOST_ROOT "/home/byli/boost_new")
set(Boost_INCLUDE_DIR "${BOOST_ROOT}/include")
set(Boost_LIBRARY_DIR "${BOOST_ROOT}/lib")

# Compiler options
set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++11")

# Link Options
# 仅为libboost_system设置动态库路径
set(LINKS_FLAGS "-L${Boost_LIBRARY_DIR} -Wl,-rpath,${Boost_LIBRARY_DIR} -lboost_system -lboost_coroutine -lcityhash -lpthread -libverbs -lmemcached")

# 设置Boost的include路径
include_directories(${Boost_INCLUDE_DIR})
set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})

# Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

# Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(sherman STATIC ${COMMON_FILE})
link_libraries(sherman)
add_executable(MemoryServer memoryserver.cpp)
target_link_libraries(MemoryServer ${LINKS_FLAGS})

# test
file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
foreach(TEST ${TEST_SRC})
    get_filename_component(TEST_NAME ${TEST} NAME_WE)
    add_executable(${TEST_NAME} ${TEST})
    target_link_libraries(${TEST_NAME} ${LINKS_FLAGS})
endforeach()