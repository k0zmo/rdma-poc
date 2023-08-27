cmake_minimum_required(VERSION 3.20)
project(rdma-poc CXX)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(Threads REQUIRED)
find_package(libfabric REQUIRED)

if(WIN32)
    add_library(getopt OBJECT src/getopt.h src/getopt.cpp)
endif()

add_executable(receiver
    src/receiver.cpp
    src/rdma_types.h
    src/rdma_defs.h
)
target_link_libraries(receiver PRIVATE
    Threads::Threads
    libfabric::libfabric
    "$<$<PLATFORM_ID:Windows>:getopt;ws2_32>"
)
target_compile_features(receiver PRIVATE cxx_std_17)

add_executable(sender
    src/sender.cpp
    src/rdma_types.h
    src/rdma_defs.h
)
target_link_libraries(sender PRIVATE
    Threads::Threads
    libfabric::libfabric
    "$<$<PLATFORM_ID:Windows>:getopt;ws2_32>"
)
target_compile_features(sender PRIVATE cxx_std_17)