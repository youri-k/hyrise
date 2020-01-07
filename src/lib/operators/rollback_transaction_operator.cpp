#include "rollback_transaction_operator.hpp"

namespace opossum {

RollbackTransactionOperator::RollbackTransactionOperator(const std::shared_ptr<const AbstractOperator>& input)
    : AbstractReadOnlyOperator(OperatorType::RollbackTransaction, input) {}

const std::string& RollbackTransactionOperator::name() const {
  static const auto name = std::string{"RollbackTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> RollbackTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<RollbackTransactionOperator>(copied_input_left);
}

void RollbackTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> RollbackTransactionOperator::_on_execute() {
  return input_table_left();
}

}  // namespace opossum
