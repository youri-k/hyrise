#include "commit_transaction_operator.hpp"

namespace opossum {

CommitTransactionOperator::CommitTransactionOperator(const std::shared_ptr<const AbstractOperator>& input)
    : AbstractReadOnlyOperator(OperatorType::CommitTransaction, input) {}

const std::string& CommitTransactionOperator::name() const {
  static const auto name = std::string{"CommitTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> CommitTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CommitTransactionOperator>(copied_input_left);
}

void CommitTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> CommitTransactionOperator::_on_execute() {
  return input_table_left();
}

}  // namespace opossum
