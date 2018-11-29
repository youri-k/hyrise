#pragma once

#include <iterator>
#include <type_traits>

#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterator.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"

namespace opossum {

template <typename IterableT>
class AnySegmentIterable;

/**
 * @brief Wraps passed segment iterable in an AnySegmentIterable
 *
 * Iterators of returned iterables will all have the same type,
 * which reduces compile times due to fewer template instantiations.
 *
 * Returns iterable if it has already been wrapped
 */
template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable);

/**
 * @brief Wraps passed segment iterable in an AnySegmentIterable in debug mode
 */
template <typename IterableT>
decltype(auto) erase_type_from_iterable_if_debug(const IterableT& iterable);

/**
 * @defgroup AnySegmentIterable Traits
 * @{
 */

template <typename ColumnDataType>
struct is_any_segment_iterable : std::false_type {};

template <typename ColumnDataType>
struct is_any_segment_iterable<AnySegmentIterable<ColumnDataType>> : std::true_type {};

template <typename IterableT>
constexpr auto is_any_segment_iterable_v = is_any_segment_iterable<IterableT>::value;
/**@}*/

template<typename ColumnDataType>
class BaseAnySegmentIterableWrapper {
 public:
  virtual ~BaseAnySegmentIterableWrapper() = default;
  virtual std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>> iterators() const = 0;
  virtual std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>> iterators(const std::shared_ptr<const PosList>& position_filter) const = 0;
  virtual size_t _on_size() const = 0;
};

template<typename ColumnDataType, typename IterableT>
class AnySegmentIterableWrapper : public BaseAnySegmentIterableWrapper<ColumnDataType> {
 public:
  explicit AnySegmentIterableWrapper(const IterableT& iterable): iterable(iterable) {}

  std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>> iterators() const override {
    auto iterators = std::optional<std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>>>{};
    iterable.with_iterators([&](auto begin, auto end) {
      iterators.emplace(begin, end);
    });
    return *iterators;
  }

  std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>> iterators(const std::shared_ptr<const PosList>& position_filter) const override {
    if constexpr (std::is_same_v<IterableT, ReferenceSegmentIterable<ColumnDataType>>) {
      Fail("Nope");
    } else {
      auto iterators = std::optional<std::pair<AnySegmentIterator<ColumnDataType>, AnySegmentIterator<ColumnDataType>>>{};
      iterable.with_iterators(position_filter, [&](auto begin, auto end) {
        iterators.emplace(begin, end);
      });
      return *iterators;
    }
  }

  size_t _on_size() const override {
    return iterable._on_size();
  }

  IterableT iterable;
};


/**
 * @brief Makes any segment iterable return type-erased iterators
 *
 * AnySegmentIterable’s sole reason for existence is compile speed.
 * Since iterables are almost always used in highly templated code,
 * the functor or lambda passed to their with_iterators methods is
 * called using many different iterators, which leads to a lot of code
 * being generated. This affects compile times. The AnySegmentIterator
 * alleviates the long compile times by erasing the iterators’ types and
 * thus reducing the number of instantiations to one (for each segment type).
 *
 * The iterators forwarded are of type AnySegmentIterator<T>. They wrap
 * any segment iterator with the cost of a virtual function call for each access.
 */
template <typename T>
class AnySegmentIterable : public PointAccessibleSegmentIterable<AnySegmentIterable<T>> {
 public:
  using ColumnDataType = T;

  template<typename IterableT>
  explicit AnySegmentIterable(const IterableT& iterable) :
    _iterable_wrapper{std::make_shared<AnySegmentIterableWrapper<T, IterableT>>(iterable)} {
    static_assert(!is_any_segment_iterable_v<IterableT>, "Iterables should not be wrapped twice.");
  }

  AnySegmentIterable(const AnySegmentIterable&) = default;
  AnySegmentIterable(AnySegmentIterable&&) = default;

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto [begin, end] = _iterable_wrapper->iterators();
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const PosList& position_filter, const Functor& functor) const {
    auto cposition_filter = std::make_shared<PosList>(position_filter.begin(), position_filter.end());
    if (position_filter.references_single_chunk()) cposition_filter->guarantee_single_chunk();
    const auto [begin, end] = _iterable_wrapper->iterators(cposition_filter);
    functor(begin, end);
  }

  size_t _on_size() const { return _iterable_wrapper->_on_size(); }

 private:
  std::shared_ptr<BaseAnySegmentIterableWrapper<T>> _iterable_wrapper;
};

template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable) {
  // clang-format off
  if constexpr(is_any_segment_iterable_v<IterableT>) {
    return iterable;
  } else {
    return AnySegmentIterable<typename IterableT::ColumnDataType>{iterable};
  }
  // clang-format on
}

template <typename IterableT>
decltype(auto) erase_type_from_iterable_if_debug(const IterableT& iterable) {
#if IS_DEBUG
  return erase_type_from_iterable(iterable);
#else
  return iterable;
#endif
}

}  // namespace opossum
