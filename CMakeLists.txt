# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.9 FATAL_ERROR)                                                     #
                                                                                                    #
project("Hyrise Example Plugin")                                                                    #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise/)                                                                           #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise/src/lib/                                                           #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/sql-parser/src                                         #
    ${TBB_INCLUDE_DIR}                                                                              #
    ${Boost_INCLUDE_DIRS}                                                                           #
)                                                                                                   #
                                                                                                    #
# Global flags and include directories                                                              #
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter       #
-Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)                                           #
                                                                                                    #
#####################################################################################################


### Modify this line to add your own plugin
add_plugin(NAME ExamplePlugin SRCS example_plugin.cpp example_plugin.hpp DEPS hyriseConsole)
