# CMake generated Testfile for 
# Source directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/libpqxx/test
# Build directory: /Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug/third_party/libpqxx/test
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(runner "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug/runner")
set_tests_properties(runner PROPERTIES  WORKING_DIRECTORY "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/debug" _BACKTRACE_TRIPLES "/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/libpqxx/test/CMakeLists.txt;78;add_test;/Users/till/HPI/Vorlesungen/7.Semester/DYOD/youri/hyrise/third_party/libpqxx/test/CMakeLists.txt;0;")
subdirs("unit")
