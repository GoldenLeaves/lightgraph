cmake_minimum_required(VERSION 3.10)

project(lightgraph)

set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-std=c++11 -lpthread -lrt -lsnappy -lgflags -llz4 -lz -lbz2 -lzstd)

include_directories(${PROJECT_SOURCE_DIR}/include/)

set(SOURCE_FILES
        include/ldb.hh
        src/ldb.cc
        include/schema.hh
        src/schema.cc
        include/properties.hh
        src/properties.cc
        include/typedef.hh
        include/strhash.hh
        src/strhash.cc
        src/iterator.cc)

set(TARGET_LINK_FLAGS -lrocksdb -ljsoncpp -ldl)

add_executable(testdb tests/testdb.cc ${SOURCE_FILES})
add_executable(test_vertex tests/test_vertex.cc ${SOURCE_FILES})
add_executable(test_lala tests/test_lala.cc)
add_executable(test_iter tests/test_iter.cc ${SOURCE_FILES})
add_executable(test_graphscan tests/test_graphscan.cc ${SOURCE_FILES})

target_link_libraries(testdb ${TARGET_LINK_FLAGS})
target_link_libraries(test_vertex ${TARGET_LINK_FLAGS})
target_link_libraries(test_iter ${TARGET_LINK_FLAGS})
target_link_libraries(test_graphscan ${TARGET_LINK_FLAGS})

