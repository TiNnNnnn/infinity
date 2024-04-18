# infinity benchmark
add_executable(skiplist_benchmark
    skiplist_benchmark.cpp
)

target_include_directories(skiplist_benchmark PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries( 
    skiplist_benchmark
    infinity_core
)