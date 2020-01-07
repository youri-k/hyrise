#include "rollback_transaction_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"

namespace opossum {

RollbackTransactionOperator::RollbackTransactionOperator()
    : AbstractReadWriteOperator(OperatorType::RollbackTransaction) {}

const std::string& RollbackTransactionOperator::name() const {
  static const auto name = std::string{"RollbackTransaction"};
  return name;
}

std::shared_ptr<AbstractOperator> RollbackTransactionOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<RollbackTransactionOperator>();
}

void RollbackTransactionOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void RollbackTransactionOperator::_on_commit_records(const CommitID commit_id) {}

void RollbackTransactionOperator::_on_rollback_records() {}

std::shared_ptr<const Table> RollbackTransactionOperator::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  if (transaction_context->is_auto_commit()) {
    FailInput("Cannot commit since there is no active transaction.");
  }

  transaction_context->rollback();

  return nullptr;
}

}  // namespace opossum
