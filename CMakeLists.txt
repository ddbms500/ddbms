cmake_minimum_required(VERSION 3.10)
project(DDBMS)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(/usr/include/mysql)
include_directories(/usr/local/include/mysql++)

add_subdirectory(Parser)
add_subdirectory(MetaData)
add_subdirectory(Executor)
add_subdirectory(Network)

add_executable(ddbms main.cpp)
target_link_libraries(ddbms boost_regex meta_data sqlparser parser)
target_link_libraries(ddbms)