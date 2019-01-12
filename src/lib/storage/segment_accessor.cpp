#include "segment_accessor.hpp"

#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

namespace detail {
template <typename T>
std::unique_ptr<BaseSegmentAccessor<T>> CreateSegmentAccessor<T>::create(
    const BaseSegment& segment) {
  std::unique_ptr<BaseSegmentAccessor<T>> accessor;
  resolve_segment_type<T>(segment, [&](const auto& typed_segment) {
    using SegmentType = std::decay_t<decltype(typed_segment)>;
    if constexpr (std::is_same_v<SegmentType, ReferenceSegment>) {
      if (typed_segment.pos_list()->references_single_chunk() && typed_segment.pos_list()->size() > 0) {
        accessor = std::make_unique<SingleChunkReferenceSegmentAccessor<T>>(typed_segment);
      } else {
        accessor = std::make_unique<MultipleChunkReferenceSegmentAccessor<T>>(typed_segment);
      }
    } else {
      if constexpr (std::is_base_of_v<BaseEncodedSegment, SegmentType>) {
        const auto compressed_vector_type = typed_segment.compressed_vector_type();
        if (compressed_vector_type) {
          resolve_decompressor_type(*compressed_vector_type, [&](const auto& decompressor) {
            using Decompressor = typename std::decay_t<decltype(*decompressor)>;
            accessor = std::make_unique<SegmentAccessor<T, SegmentType, Decompressor>>(typed_segment);
          });
        } else {
          accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
        }
      } else {
        accessor = std::make_unique<SegmentAccessor<T, SegmentType>>(typed_segment);
      }
    }
  });
  return accessor;
}
EXPLICITLY_INSTANTIATE_DATA_TYPES(CreateSegmentAccessor);
}  // namespace detail

}  // namespace opossum
