#pragma once

#include "pos_list.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/base_segment.hpp"

namespace opossum {

template<typename ColumnDataType, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter, const Functor& functor) {
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<ColumnDataType>(base_segment);
  any_segment_iterable.with_iterators(position_filter, functor);
#else

#endif
}

}  // namespace opossum
