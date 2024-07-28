cmake_minimum_required (VERSION 3.23)

project ("BSocketLib")

set(CMAKE_CXX_STANDARD 20)
set(LIBS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libs)
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/libs_dep.json)
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/libs_dep.json LIBS_DEP_JSON)
string(JSON total_libs LENGTH "${LIBS_DEP_JSON}" libs)
MATH(EXPR total_libs "${total_libs}-1")
foreach(IDX RANGE ${total_libs})
    # Get the name from the current JSON element.
    string(JSON CUR_NAME GET ${LIBS_DEP_JSON} libs ${IDX} name)
    # Get the URL from the current JSON element.
    string(JSON TARGET_LINK_PRJ GET ${LIBS_DEP_JSON} libs ${IDX} prj_name)
    if (NOT TARGET ${TARGET_LINK_PRJ})
       add_subdirectory(${LIBS_DIR}/${CUR_NAME} ${CMAKE_BINARY_DIR}/${CUR_NAME})
    endif()
    list(APPEND ${PROJECT_NAME}_TARGETS_LINK_PRJ ${TARGET_LINK_PRJ})
endforeach()
endif()

# Add source to this project's executable.
set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
file(GLOB_RECURSE SOURCES CONFIGURE_DEPENDS ${SRC_DIR}/*.cpp)
set(INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
file(GLOB_RECURSE HEADERS CONFIGURE_DEPENDS ${INCLUDE_DIR}/*.h)

set(PREBUILT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib)
find_library(MYSELF_FIND NAMES ${PROJECT_NAME} PATHS ${PREBUILT_PATH})

if (MYSELF_FIND)
message("found prebuilt library ${MYSELF_FIND}")
message("use variable PREBUILT_${PROJECT_NAME} to obtain lib path")
message("use variable ${PROJECT_NAME}_INCLUDE to obtain include dir")
set(PREBUILT_${PROJECT_NAME} ${MYSELF_FIND} CACHE INTERNAL "lib path")
set(${PROJECT_NAME}_INCLUDE ${INCLUDE_DIR} CACHE INTERNAL "lib include")
else()
message("building library ${PROJECT_NAME}")
include_directories(${PROJECT_NAME} PUBLIC ${INCLUDE_DIR})
add_library(${PROJECT_NAME} ${SOURCES})
target_sources(${PROJECT_NAME} PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${INCLUDE_DIR}
    FILES ${HEADERS}
)
set_target_properties(${PROJECT_NAME} PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib
)
foreach(TARGET_LINK_LIB ${${PROJECT_NAME}_TARGETS_LINK_PRJ})
target_link_libraries(${PROJECT_NAME} ${TARGET_LINK_LIB})
endforeach()
if (TARGET CommonLib)
target_precompile_headers(${PROJECT_NAME} REUSE_FROM CommonLib)
else()
message(FATAL_ERROR "This lib depends on CommonLib and must have dependant first!")
endif()
endif()