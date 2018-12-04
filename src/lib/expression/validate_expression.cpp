#include "validate_expression.hpp"

#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

ValidateExpression::ValidateExpression():
  AbstractExpression(ExpressionType::Validate, {}) {}

std::shared_ptr<AbstractExpression> ValidateExpression::deep_copy() const {
  return std::make_shared<ValidateExpression>();
}

std::string ValidateExpression::as_column_name() const {
  return "[Validate]";
}

DataType ValidateExpression::data_type() const {
  return ExpressionEvaluator::DataTypeBool;
}

bool ValidateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return true;
}

size_t ValidateExpression::_on_hash() const {
  return 0;
}


} // namespace opossum
