# CMake generated Testfile for 
# Source directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import
# Build directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug/third_party/nlohmann_json/test/cmake_import
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(cmake_import_configure "/usr/local/Cellar/cmake/3.15.5/bin/cmake" "-G" "Unix Makefiles" "-Dnlohmann_json_DIR=/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug/third_party/nlohmann_json" "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import/project")
set_tests_properties(cmake_import_configure PROPERTIES  FIXTURES_SETUP "cmake_import" _BACKTRACE_TRIPLES "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import/CMakeLists.txt;1;add_test;/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import/CMakeLists.txt;0;")
add_test(cmake_import_build "/usr/local/Cellar/cmake/3.15.5/bin/cmake" "--build" ".")
set_tests_properties(cmake_import_build PROPERTIES  FIXTURES_REQUIRED "cmake_import" _BACKTRACE_TRIPLES "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import/CMakeLists.txt;7;add_test;/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/nlohmann_json/test/cmake_import/CMakeLists.txt;0;")
