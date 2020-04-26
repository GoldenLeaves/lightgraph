cmake_minimum_required(VERSION 3.10)

project(lightgraph)

set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-std=c++11 -lpthread -lrt -lsnappy -lgflags -llz4 -lz -lbz2 -lzstd)

include_directories(${PROJECT_SOURCE_DIR}/include/)

set(SOURCE_FILES
        src/ldb.cc
        src/iterator.cc
        src/schema.cc
        src/properties.cc
        src/strhash.cc
        src/endian_conversion.cc)
set(HEADER_FILES
        include/ldb.hh
        include/schema.hh
        include/properties.hh
        include/typedef.hh
        include/strhash.hh
        include/endian_conversion.hh)
set(FILES
        ${SOURCE_FILES}
        ${HEADER_FILES})

set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
add_library(lightgraph SHARED ${FILES})
add_library(lightgraph_static STATIC ${FILES})
set_target_properties(lightgraph_static PROPERTIES OUTPUT_NAME "lightgraph")
set_target_properties(lightgraph PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties(lightgraph_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

set(TARGET_LINK_FLAGS -lrocksdb -ljsoncpp -ldl -pthread)

add_executable(testdb tests/testdb.cc ${FILES})
add_executable(test_vertex tests/test_vertex.cc ${FILES})
add_executable(test_lala tests/test_lala.cc)
add_executable(test_iter tests/test_iter.cc ${FILES})
add_executable(test_graphscan tests/test_graphscan.cc ${FILES})
add_executable(test_prefix_extractor tests/test_prefix_extractor.cc ${FILES})
add_executable(experiment1 tests/experiment1.cc ${FILES})
add_executable(experiment2 tests/experiment2.cc ${FILES})
add_executable(experiment3 tests/experiment3.cc ${FILES})
add_executable(experiment4 tests/experiment4.cc ${FILES})
add_executable(experiment5 tests/experiment5.cc ${FILES})

target_link_libraries(testdb ${TARGET_LINK_FLAGS})
target_link_libraries(test_vertex ${TARGET_LINK_FLAGS})
target_link_libraries(test_iter ${TARGET_LINK_FLAGS})
target_link_libraries(test_graphscan ${TARGET_LINK_FLAGS})
target_link_libraries(test_prefix_extractor ${TARGET_LINK_FLAGS})
target_link_libraries(experiment1 ${TARGET_LINK_FLAGS})
target_link_libraries(experiment2 ${TARGET_LINK_FLAGS})
target_link_libraries(experiment3 ${TARGET_LINK_FLAGS})
target_link_libraries(experiment4 ${TARGET_LINK_FLAGS})
target_link_libraries(experiment5 ${TARGET_LINK_FLAGS})
