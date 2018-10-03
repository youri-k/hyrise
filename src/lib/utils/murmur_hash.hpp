#pragma once

#include <string>
#include <type_traits>

namespace opossum {

unsigned int murmur_hash2(const void* key, unsigned int len, unsigned int seed);

// murmur hash for builtin number types (int, double)
template <typename T>
std::enable_if_t<std::is_arithmetic_v<T>, unsigned int> murmur2(T key, unsigned int seed) {
  return murmur_hash2(&key, sizeof(T), seed);
}

// murmur hash for std::string
template <typename T>
std::enable_if_t<std::is_same_v<T, std::string>, unsigned int> murmur2(T key, unsigned int seed) {
  return murmur_hash2(key.c_str(), key.size(), seed);
}

// murmur hash for std::string_view
template <typename T>
typename std::enable_if<std::is_same<T, std::string_view>::value, unsigned int>::type murmur2(T key,
                                                                                              unsigned int seed) {
  return murmur_hash2(key.data(), key.size(), seed);
}

}  // namespace opossum
