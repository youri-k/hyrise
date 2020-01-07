#pragma once

#include "abstract_operator.hpp"
#include "abstract_read_write_operator.hpp"

namespace opossum {

class BeginTransactionOperator : public AbstractReadWriteOperator {
 public:
  explicit BeginTransactionOperator(const std::shared_ptr<const AbstractOperator>& input);

  const std::string& name() const override;

  void _on_commit_records(const CommitID commit_id) override;

  void _on_rollback_records() override;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
};
}  // namespace opossum
