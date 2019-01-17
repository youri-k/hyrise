#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * SQL's list used as the right operand of IN
 */
class ListExpression : public AbstractExpression {
 public:
  explicit ListExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements);

  DataType data_type() const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& elements() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
