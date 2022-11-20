add_library(executor STATIC Executor.cpp)
target_link_libraries(executor mysqlpp)