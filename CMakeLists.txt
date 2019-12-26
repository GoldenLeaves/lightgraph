cmake_minimum_required(VERSION 3.10)

project(lightgraph)

set(CMAKE_CXX_FLAGS_RELEASE "-O2")

add_compile_options(-std=c++11 -lpthread -lrt -lsnappy -lgflags -llz4 -lz -lbz2 -lzstd)

add_executable(testdb tests/testdb.cc)
target_link_libraries(testdb -lrocksdb)
TARGET_LINK_LIBRARIES(testdb -ldl)