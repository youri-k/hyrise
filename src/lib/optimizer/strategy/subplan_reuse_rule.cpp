#include "subplan_reuse_rule.hpp"

#include <unordered_map>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

struct LQPHash {
  bool operator()(const std::shared_ptr<AbstractLQPNode>& root) const { return root->hash(); }
};

struct LQPEquals {
  bool operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const {
    return *lhs == *rhs;
  }
};

struct SubplanReplacement {
  std::shared_ptr<AbstractLQPNode> parent;
  LQPInputSide input_side;
  std::shared_ptr<AbstractLQPNode> replacement_plan;
};

using SubplanEqualityMapping = std::unordered_map<std::shared_ptr<AbstractLQPNode>,
                                                  std::vector<std::shared_ptr<AbstractLQPNode>>, LQPHash, LQPEquals>;
using SubplanReplacementMapping = std::vector<SubplanReplacement>;
using ColumnReplacementMapping = std::unordered_map<LQPColumnReference, LQPColumnReference>;

std::unordered_set<LQPColumnReference> get_column_references(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto column_references = std::unordered_set<LQPColumnReference>{};
  for (const auto& expression : expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        column_references.emplace(column_expression->column_reference);
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
  return column_references;
}

ColumnReplacementMapping create_column_mapping(const AbstractLQPNode& from_node, const AbstractLQPNode& to_node) {
  auto mapping = std::unordered_map<LQPColumnReference, LQPColumnReference>{};

  const auto& from_expressions = from_node.column_expressions();
  const auto& to_expressions = to_node.column_expressions();

  Assert(from_expressions.size() == to_expressions.size(), "Expected same number of expressions");

  const auto traverse_expressions = [&](const auto& expression_a, const auto& expression_b) {
    if (!expression_a) {
      return;
    }

    Assert(expression_a->type == expression_b->type, "Expected same type");

    if (const auto column_expression_a = std::dynamic_pointer_cast<LQPColumnExpression>(expression_a)) {
      const auto column_expression_b = std::dynamic_pointer_cast<LQPColumnExpression>(expression_b);
      mapping.emplace(column_expression_a->column_reference, column_expression_b->column_reference);
    }
  };

  for (auto column_id = ColumnID{0}; column_id < from_expressions.size(); ++column_id) {
    traverse_expressions(from_expressions[column_id], to_expressions[column_id]);
  }

  return mapping;
}

void add_column_replacements(ColumnReplacementMapping& column_replacements, const AbstractLQPNode& obsolete_plan,
                             const AbstractLQPNode& replacement_plan) {
  const auto& obsolete_expressions = obsolete_plan.column_expressions();
  const auto& replacements_expressions = replacement_plan.column_expressions();

  Assert(obsolete_expressions.size() == replacements_expressions.size(), "Expected same number of expressions");

  const auto traverse_expressions = [&](const auto& expression_a, const auto& expression_b) {
    if (!expression_a) {
      return;
    }

    Assert(expression_a->type == expression_b->type, "Expected same type");

    if (const auto column_expression_a = std::dynamic_pointer_cast<LQPColumnExpression>(expression_a)) {
      const auto column_expression_b = std::dynamic_pointer_cast<LQPColumnExpression>(expression_b);
      column_replacements.emplace(column_expression_a->column_reference, column_expression_b->column_reference);
    }
  };

  for (auto column_id = ColumnID{0}; column_id < obsolete_expressions.size(); ++column_id) {
    traverse_expressions(obsolete_expressions[column_id], replacements_expressions[column_id]);
  }
}

void apply_column_replacement_mapping(std::shared_ptr<AbstractExpression>& expression,
                                      const ColumnReplacementMapping& column_replacement_mapping) {
  visit_expression(expression, [&](auto& sub_expression) {
    if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
      const auto column_replacement_iter = column_replacement_mapping.find(column_expression->column_reference);
      if (column_replacement_iter != column_replacement_mapping.end()) {
        sub_expression = std::make_shared<LQPColumnExpression>(column_replacement_iter->second);
      }
    }
    return ExpressionVisitation::VisitArguments;
  });
}

void apply_column_replacement_mapping(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                      const ColumnReplacementMapping& column_replacement_mapping) {
  for (auto& expression : expressions) {
    apply_column_replacement_mapping(expression, column_replacement_mapping);
  }
}

bool is_join_predicate_ambiguous_with_reuse(const JoinNode& join_node,
                                            const ColumnReplacementMapping& column_replacement_mapping) {
  /**
   * TODO: Only rewrite expressions from the input side that we're coming from
   */

  auto expressions_left = expressions_deep_copy(join_node.left_input()->column_expressions());
  auto expressions_right = expressions_deep_copy(join_node.right_input()->column_expressions());

  apply_column_replacement_mapping(expressions_left, column_replacement_mapping);
  apply_column_replacement_mapping(expressions_right, column_replacement_mapping);

  const auto is_join_predicate_ambiguous = [&](const auto& predicate) {
    const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
    Assert(binary_predicate, "Expected join predicate to be binary");

    const auto find_column_id = [](const auto& column_expressions, const auto& expression) {
      for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); ++column_id) {
        if (*column_expressions[column_id] == expression) return true;
      }
      return false;
    };

    const auto left_in_left = find_column_id(expressions_left, *binary_predicate->arguments[0]);
    const auto left_in_right = find_column_id(expressions_right, *binary_predicate->arguments[0]);
    const auto right_in_left = find_column_id(expressions_left, *binary_predicate->arguments[1]);
    const auto right_in_right = find_column_id(expressions_right, *binary_predicate->arguments[1]);

    // Predicate is ambiguous when: No argument refers to an input side OR multiple arguments refer to an input
    if (left_in_left && right_in_right) {
      return left_in_right || right_in_left;
    } else if (left_in_right && right_in_left) {
      return left_in_left || right_in_right;
    } else {
      return true;
    }
  };

  for (const auto& join_predicate : join_node.join_predicates()) {
    auto rewritten_join_predicate = join_predicate->deep_copy();
    apply_column_replacement_mapping(rewritten_join_predicate, column_replacement_mapping);
    if (is_join_predicate_ambiguous(rewritten_join_predicate)) {
      return true;
    }
  }

  return false;
}

bool is_join_output_reduced_with_reuse(const JoinNode& join_node,
                                       const ColumnReplacementMapping& column_replacement_mapping) {
  auto column_expressions = expressions_deep_copy(join_node.column_expressions());
  const auto pre_distinct_column_count = get_column_references(column_expressions).size();

  apply_column_replacement_mapping(column_expressions, column_replacement_mapping);
  const auto post_distinct_column_count = get_column_references(column_expressions).size();

  return pre_distinct_column_count != post_distinct_column_count;
}

bool is_reuse_possible(const std::shared_ptr<AbstractLQPNode>& parent, const LQPInputSide input_side,
                       const std::shared_ptr<AbstractLQPNode>& replacement) {
  auto result = true;

  const auto candidate = parent->input(input_side);

  std::cout << "Testing replacement of \n" << *candidate << " with\n " << *replacement << std::endl;

  const auto column_mapping = create_column_mapping(*candidate, *replacement);

  visit_lqp_upwards(parent, [&](const auto& node) {
    if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_mode != JoinMode::Cross) {
        if (is_join_predicate_ambiguous_with_reuse(*join_node, column_mapping)) {
          std::cout << "NOT POSSIBLE BECAUSE OF PREDICATE of "<< join_node->description() << std::endl;
          result = false;
        }
      }

      if (is_join_output_reduced_with_reuse(*join_node, column_mapping)) {
        std::cout << "NOT POSSIBLE BECAUSE OF OUTPUT of " << join_node->description() << std::endl;
        result = false;
      }

      if (result) std::cout << "OKAY WITH " << join_node->description() << std::endl;

      return result ? LQPVisitation::VisitInputs : LQPVisitation::DoNotVisitInputs;
    }

    return LQPVisitation::VisitInputs;
  });

  return result;
}

void traverse(const std::shared_ptr<AbstractLQPNode>& parent, const LQPInputSide input_side,
              std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited,
              const SubplanEqualityMapping& equal_subplans, SubplanReplacementMapping& subplan_mapping) {
  const auto node = parent->input(input_side);
  if (!node) {
    return;
  }

  const auto candidate_plans_iter = equal_subplans.find(node);
  Assert(candidate_plans_iter != equal_subplans.end(), "Expected an entry for each subplan");
  const auto candidate_plans = candidate_plans_iter->second;

  for (const auto& candidate_plan : candidate_plans) {
    if (candidate_plan == node) {
      break;
    }

    if (is_reuse_possible(parent, input_side, candidate_plan)) {
      subplan_mapping.emplace_back(SubplanReplacement{parent, input_side, candidate_plan});
      return;
    }
  }

  traverse(node, LQPInputSide::Left, visited, equal_subplans, subplan_mapping);
  traverse(node, LQPInputSide::Right, visited, equal_subplans, subplan_mapping);
}

}  // namespace

namespace opossum {

void SubplanReuseRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  /**
   * Create groups of equal subplans
   */
  auto equal_subplans = SubplanEqualityMapping{};
  visit_lqp(root, [&](const auto& subplan) {
    auto equal_subplans_iter = equal_subplans.find(subplan);
    if (equal_subplans_iter == equal_subplans.end()) {
      equal_subplans.emplace(subplan, SubplanEqualityMapping::mapped_type{subplan});
      return LQPVisitation::VisitInputs;
    } else {
      equal_subplans_iter->second.emplace_back(subplan);
    }

    return LQPVisitation::VisitInputs;
  });

  /**
   * Find possible reuses
   */
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  auto subplan_replacements = SubplanReplacementMapping{};
  traverse(root, LQPInputSide::Left, visited_nodes, equal_subplans, subplan_replacements);

  if (subplan_replacements.empty()) {
    return;
  }

  /**
   * Replace subplans
   */
  auto column_replacements = ColumnReplacementMapping{};
  for (const auto& [parent, input_side, replacement_plan] : subplan_replacements) {
    const auto obsolete_plan = parent->input(input_side);
    add_column_replacements(column_replacements, *obsolete_plan, *replacement_plan);

    parent->set_input(input_side, replacement_plan);
  }

  /**
   * Fix all expressions that referred to now-removed subplans
   */
  visit_lqp(root, [&](const auto& node) {
    for (auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](auto& sub_expression) {
        if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
          const auto column_replacement_iter = column_replacements.find(column_expression->column_reference);
          if (column_replacement_iter != column_replacements.end()) {
            sub_expression = std::make_shared<LQPColumnExpression>(column_replacement_iter->second);
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
