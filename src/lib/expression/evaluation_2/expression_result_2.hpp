#pragma once

#include <vector>

#include "boost/variant.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "expression_result_views_2.hpp"
#include "null_value.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables/segment_positions.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseExpressionResult2 {
 public:
  BaseExpressionResult2() = default;
  virtual ~BaseExpressionResult2() = default;
  BaseExpressionResult2(const BaseExpressionResult2&) = default;
  BaseExpressionResult2(BaseExpressionResult2&&) = default;
  BaseExpressionResult2& operator=(const BaseExpressionResult2&) = default;
  BaseExpressionResult2& operator=(BaseExpressionResult2&&) = default;

  virtual AllTypeVariant value_as_variant(const size_t idx) const = 0;
};

/**
 * The typed result of a (Sub)Expression.
 * Wraps a vector of `values` and a vector of `nulls` that are filled differently, with the possible combinations best
 * explained by the examples below
 *
 * values
 *      Contains a value for each row if the result is a Series
 *      Contains a single value if the result is a Literal
 *
 * nulls
 *      Is empty if the ExpressionResult2 is non-nullable
 *      Contains a bool for each element of `values` if the ExpressionResult2 is nullable
 *      Contains a single element that the determines whether all elements are either null or not
 *
 * Examples:
 *      {values: [1, 2, 3, 4]; nulls: []} --> Series [1, 2, 3, 4]
 *      {values: [1, 2, 3, 4]; nulls: [false]} --> Series [1, 2, 3, 4]
 *      {values: [1, 2, 3, 4]; nulls: [true]} --> Literal [NULL]
 *      {values: [1, 2, 3, 4]; nulls: [true, false, true, false]} --> Series [NULL, 2, NULL, 4]
 *      {values: [1]; nulls: []} --> Literal [1]
 *      {values: [1]; nulls: [true]} --> Literal [NULL]
 *
 * Often the ExpressionEvaluator will compute nulls and values independently, which is why states with redundant
 * information, such as `{values: [1, 2, 3, 4]; nulls: [true]}` or `{values: [1, 2, 3, 4]; nulls: [false]}`, are legal.
 */
template <typename T>
class ExpressionResult2 : public BaseExpressionResult2 {
 public:
  using Type = T;

  static std::shared_ptr<ExpressionResult2<T>> make_null() {
    ExpressionResult2<T> null_value({{T{}}}, {true});
    return std::make_shared<ExpressionResult2<T>>(null_value);
  }

  ExpressionResult2() = default;

  explicit ExpressionResult2(std::vector<T> values, std::vector<bool> nulls = {})
      : values(std::move(values)), nulls(std::move(nulls)) {
    DebugAssert(nulls.empty() || nulls.size() == values.size(), "Need as many nulls as values or no nulls at all");
  }

  bool is_nullable_series() const { return size() != 1; }
  bool is_literal() const { return size() == 1; }
  bool is_nullable() const { return !nulls.empty(); }

  const T& value(const size_t idx) const {
    DebugAssert(size() == 1 || idx < size(), "Invalid ExpressionResult2 access");
    return values[std::min(idx, values.size() - 1)];
  }

  AllTypeVariant value_as_variant(const size_t idx) const final {
    return is_null(idx) ? AllTypeVariant{NullValue{}} : AllTypeVariant{value(idx)};
  }

  bool is_null(const size_t idx) const {
    DebugAssert(size() == 1 || idx < size(), "Null idx out of bounds");
    if (nulls.empty()) return false;
    return nulls[std::min(idx, nulls.size() - 1)];
  }

  /**
   * Resolve ExpressionResult2<T> to ExpressionResultNullableSeries<T>, ExpressionResultNonNullSeries<T> or
   * ExpressionResultLiteral<T>
   *
   * Once resolved, a View doesn't need to do bounds checking when queried for value() or is_null(), thus reducing
   * overhead
   */
  template <typename Functor>
  void as_view(const Functor& fn) const {
    if (size() == 1) {
      fn(ExpressionResultLiteral(values.front(), is_nullable() && nulls.front()));
    } else if (nulls.size() == 1 && nulls.front()) {
      fn(ExpressionResultLiteral(T{}, true));
    } else if (!is_nullable()) {
      fn(ExpressionResultNonNullSeries(values));
    } else {
      fn(ExpressionResultNullableSeries(values, nulls));
    }
  }

  size_t size() const { return values.size(); }

  std::vector<T> values;
  std::vector<bool> nulls;
};

}  // namespace opossum
