#pragma once

#include <stdint.h>

namespace opossum {

// Precedence levels for parenthesizing expression arguments. See AbstractExpression::_enclose_argument_description(AbstractExpression::DescriptionMode::ColumnName)
enum class ExpressionPrecedence : uint32_t {
  Highest = 0,
  UnaryPredicate,
  MultiplicationDivision,
  AdditionSubtraction,
  BinaryTernaryPredicate,
  Logical
};

}  // namespace opossum
