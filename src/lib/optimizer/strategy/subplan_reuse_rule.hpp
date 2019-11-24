#pragma once

#include <unordered_set>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

// TODO Doc
//  * We do not optimize across subqueries as lineage is complicated enough

class SubplanReuseRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;
};

}  // namespace opossum
