#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * A PredicateNode with a ValidateExpression as predicate represents a MVCC validation in the LQP.
 *
 * After all, a MVCC validation is nothing more but a predicate on a specialized set of columns.
 */
class ValidateExpression : public AbstractExpression {
 public:
  ValidateExpression();

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
