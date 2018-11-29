#pragma once

#include "pos_list.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/base_segment.hpp"

namespace opossum {

template<typename T, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter, const Functor& functor) {
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
  any_segment_iterable.with_iterators(position_filter, functor);
#else

#endif
}

template<typename T, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const Functor& functor) {
  segment_with_iterators(base_segment, nullptr, functor);
}

template<typename T, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter, const Functor& functor) {
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
  any_segment_iterable.for_each(position_filter, functor);
#else

#endif
}

template<typename T, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const Functor& functor) {
  segment_for_each(base_segment, nullptr, functor);
}

}  // namespace opossum
