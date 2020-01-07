#include "begin_transaction_operator.hpp"

namespace opossum {

BeginTransactionOperator::BeginTransactionOperator(const std::shared_ptr<const AbstractOperator>& input)
    : AbstractReadOnlyOperator(OperatorType::BeginTransaction, input) {}

const std::string& BeginTransactionOperator::name() const {
  static const auto name = std::string{"BeginTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> BeginTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<BeginTransactionOperator>(copied_input_left);
}

void BeginTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> BeginTransactionOperator::_on_execute() {
  DebugAssert()
  return input_table_left();
}

}  // namespace opossum
