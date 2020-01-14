# CMake generated Testfile for 
# Source directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory
# Build directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug/third_party/nlohmann_json/test/cmake_add_subdirectory
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(cmake_add_subdirectory_configure "/usr/local/Cellar/cmake/3.15.5/bin/cmake" "-G" "Unix Makefiles" "-Dnlohmann_json_source=/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json" "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory/project")
set_tests_properties(cmake_add_subdirectory_configure PROPERTIES  FIXTURES_SETUP "cmake_add_subdirectory" _BACKTRACE_TRIPLES "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory/CMakeLists.txt;1;add_test;/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory/CMakeLists.txt;0;")
add_test(cmake_add_subdirectory_build "/usr/local/Cellar/cmake/3.15.5/bin/cmake" "--build" ".")
set_tests_properties(cmake_add_subdirectory_build PROPERTIES  FIXTURES_REQUIRED "cmake_add_subdirectory" _BACKTRACE_TRIPLES "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory/CMakeLists.txt;7;add_test;/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_add_subdirectory/CMakeLists.txt;0;")
