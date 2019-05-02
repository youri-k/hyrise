#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class ExpressionEvaluatorRangeMode {
  All, Whitelist, Blacklist
};

struct BaseExpressionEvaluatorRange {
  size_t begin{};
  size_t end{};
  ExpressionEvaluatorRangeMode mode{ExpressionEvaluatorRangeMode::All};
};

struct ExpressionEvaluatorPosListRange : BaseExpressionEvaluatorRange {
  std::vector<ChunkOffset> offsets{}
};

}  // namespace opossum
