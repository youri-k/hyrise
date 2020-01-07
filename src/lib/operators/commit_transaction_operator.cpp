#include "commit_transaction_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"

namespace opossum {

CommitTransactionOperator::CommitTransactionOperator(const std::shared_ptr<const AbstractOperator>& input)
    : AbstractReadWriteOperator(OperatorType::CommitTransaction, input) {}

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

void CommitTransactionOperator::_on_commit_records(const CommitID commit_id) {};

void CommitTransactionOperator::_on_rollback_records() {};

std::shared_ptr<const Table> CommitTransactionOperator::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  if (transaction_context->is_auto_commit()) {
    FailInput("Cannot commit since there is no active transaction.");
  }

  transaction_context->commit();

  transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  return input_table_left();
}

}  // namespace opossum
