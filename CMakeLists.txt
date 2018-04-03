cmake_minimum_required(VERSION 3.9)
project(parquet_metadata_dump)

set(CMAKE_CXX_STANDARD 11)

include_directories(include)
link_directories(lib)
set(PARQUET_THRIFT_FILES
        gen-cpp/parquet_constants.cpp
        gen-cpp/parquet_constants.h
        gen-cpp/parquet_types.cpp
        gen-cpp/parquet_types.h
        )

include_directories(${CMAKE_CURRENT_BINARY_DIR})


add_custom_command(OUTPUT ${PARQUET_THRIFT_FILES}
        COMMAND thrift --gen cpp ${PROJECT_SOURCE_DIR}/parquet.thrift
        )


add_executable(parquet_metadata_dump main.cpp
        ${PARQUET_THRIFT_FILES})
target_link_libraries(parquet_metadata_dump thrift)