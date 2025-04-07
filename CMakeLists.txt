cmake_minimum_required(VERSION 3.20)
project(rdma-poc CXX)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)
find_package(libfabric REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)

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

add_library(asio::asio IMPORTED INTERFACE)
set_target_properties(asio::asio PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
    "${CMAKE_CURRENT_SOURCE_DIR}/vendor/asio/asio/include"
)

add_executable(efa_sender src/efa_sender.cpp)
target_link_libraries(efa_sender PRIVATE
    Threads::Threads
    libfabric::libfabric
    concurrentqueue::concurrentqueue
    "$<$<PLATFORM_ID:Windows>:getopt;ws2_32>"
    asio::asio
)
target_compile_features(efa_sender PRIVATE cxx_std_17)

add_executable(efa_receiver src/efa_receiver.cpp)
target_link_libraries(efa_receiver PRIVATE
    Threads::Threads
    libfabric::libfabric
    concurrentqueue::concurrentqueue
    "$<$<PLATFORM_ID:Windows>:getopt;ws2_32>"
    asio::asio
)
target_compile_features(efa_sender PRIVATE cxx_std_17)

