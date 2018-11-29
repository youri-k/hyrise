#include "create_iterable_from_segment.hpp"

namespace opossum {

template<typename ColumnDataType>
AnySegmentIterable<ColumnDataType> create_any_segment_iterable(const BaseSegment& base_segment) {
  auto any_segment_iterable = std::optional<AnySegmentIterable<ColumnDataType>>{};

  resolve_segment_type<ColumnDataType>(base_segment, [&](const auto& segment) {
    const auto iterable = create_iterable_from_segment<ColumnDataType>(segment);
    any_segment_iterable.emplace(erase_type_from_iterable(iterable));
  });

  return std::move(*any_segment_iterable);
}

template AnySegmentIterable<int32_t> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<int64_t> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<float> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<double> create_any_segment_iterable(const BaseSegment& base_segment);
template AnySegmentIterable<std::string> create_any_segment_iterable(const BaseSegment& base_segment);

}  // namespace opossum
