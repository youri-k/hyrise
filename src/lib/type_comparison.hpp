#pragma once

#include <boost/lexical_cast.hpp>
#include <functional>
#include <string>
#include <type_traits>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// source: http://stackoverflow.com/questions/16893992/check-if-type-can-be-explicitly-converted
template <class From, class To>
struct is_explicitly_convertible {
  enum { value = std::is_constructible<To, From>::value && !std::is_convertible<From, To>::value };
};

// source: http://stackoverflow.com/questions/27709461/check-if-type-can-be-an-argument-to-boostlexical-caststring
template <typename T, typename = void>
struct IsLexCastable : std::false_type {};

template <typename T>
struct IsLexCastable<T, decltype(void(std::declval<std::ostream&>() << std::declval<T>()))> : std::true_type {};

template <typename L, typename R, typename = void>
struct HaveCommonType : std::false_type {};

template <typename L, typename R>
struct HaveCommonType<L, R, std::void_t<std::common_type<L, R>>> : std::true_type {};

template <typename L, typename R, typename = void>
struct have_common_type : std::false_type {};

template <typename L, typename R>
struct have_common_type<L, R, std::void_t<std::common_type<L, R>>> : std::true_type {};

template <typename T>
inline constexpr bool have_common_type_v = have_common_type<T>::value;

/* EQUAL */
// L and R are implicitly convertible
typename std::enable_if_t<have_common_type_v<L, R>, bool> value_equal(const L& l, const R& r) {
  return l == r;
}

// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_equal(const L& l,
                                                                                                                const R& r) {
  return boost::lexical_cast<L>(r) == l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_equal(const L& l,
                                                                                                                const R& r) {
  return boost::lexical_cast<R>(l) == r;
}

/* SMALLER */
// L and R are implicitly convertible
template <typename L, typename R>
  return l < r;
}
// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_smaller(const L& l,
                                                                                                                  const R& r) {
  return boost::lexical_cast<L>(r) < l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_smaller(const L& l,
                                                                                                                  const R& r) {
  return boost::lexical_cast<R>(l) < r;
}

/* GREATER > */
// L and R are implicitly convertible
template <typename L, typename R>
  return l > r;
}
// L is arithmetic, R is explicitly convertible to L
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<L> && is_lex_castable_v<R> && !std::is_arithmetic_v<R>, bool> value_greater(const L& l,
                                                                                                                  const R& r) {
  return boost::lexical_cast<L>(r) > l;
}

// R is arithmetic, L is explicitly convertible to R
template <typename L, typename R>
std::enable_if_t<std::is_arithmetic_v<R> && is_lex_castable_v<L> && !std::is_arithmetic_v<L>, bool> value_greater(const L& l,
                                                                                                                  const R& r) {
  return boost::lexical_cast<R>(l) > r;
}

// Function that calls a given functor with the correct std comparator
template <typename Functor>
void with_comparator(const PredicateCondition predicate_condition, const Functor& func) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return func(std::equal_to<void>{});

    case PredicateCondition::NotEquals:
      return func(std::not_equal_to<void>{});

    case PredicateCondition::LessThan:
      return func(std::less<void>{});

    case PredicateCondition::LessThanEquals:
      return func(std::less_equal<void>{});

    case PredicateCondition::GreaterThan:
      return func(std::greater<void>{});

    case PredicateCondition::GreaterThanEquals:
      return func(std::greater_equal<void>{});

    default:
      Fail("Unsupported operator.");
  }
}

}  // namespace opossum
