#include "subplan_reuse_rule.hpp"

#include <unordered_map>

#include "expression/lqp_column_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

struct LQPHash {
  bool operator()(const std::shared_ptr<AbstractLQPNode>& root) const {
    return root->hash();
  }
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

using SubplanEqualityMapping = std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<AbstractLQPNode>>, LQPHash, LQPEquals>;
using SubplanReplacementMapping = std::vector<SubplanReplacement>;
using ColumnReplacementMapping = std::unordered_map<LQPColumnReference, LQPColumnReference>;

std::unordered_set<LQPColumnReference> get_column_references(const AbstractLQPNode& node) {
  auto column_references = std::unordered_set<LQPColumnReference>{};
  for (const auto& node_expression : node.column_expressions()) {
    visit_expression(node_expression, [&](const auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        column_references.emplace(column_expression->column_reference);
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
  return column_references;
}

bool expressions_contain_any_column_reference(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
    const std::unordered_set<LQPColumnReference>& column_reference_blacklist) {
  auto conflict = false;
  for (const auto& expression : expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        if (column_reference_blacklist.count(column_expression->column_reference)) {
          conflict = true;
          return ExpressionVisitation::DoNotVisitArguments;
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
  return conflict;
}

void add_column_replacements(ColumnReplacementMapping& column_replacements, const AbstractLQPNode& obsolete_plan, const AbstractLQPNode& replacement_plan) {
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

void traverse(
    const std::shared_ptr<AbstractLQPNode>& parent,
    const LQPInputSide input_side,
    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited,
    const std::unordered_set<LQPColumnReference>& column_blacklist,
    const SubplanEqualityMapping& equal_subplans,
    SubplanReplacementMapping& subplan_mapping) {

  const auto input = parent->input(input_side);
  if (!input) {
    return;
  }

  if (!visited.emplace(input).second) {
    return;
  }

  const auto candidate_plans_iter = equal_subplans.find(input);
  Assert(candidate_plans_iter != equal_subplans.end(), "Expected an entry for each subplan");

  const auto candidate_plans = candidate_plans_iter->second;

  for (const auto& candidate_plan : candidate_plans) {
    if (candidate_plan == input) {
      break;
    }

    if (expressions_contain_any_column_reference(candidate_plan->column_expressions(), column_blacklist)) {
      continue;
    }

    subplan_mapping.emplace_back(SubplanReplacement{parent, input_side, candidate_plan});
    return;
  }

  if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(input)) {
    for (const auto next_input_side : {LQPInputSide::Left, LQPInputSide::Right}) {
      const auto opposite_input_side = next_input_side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;

      switch (join_node->join_mode) {
        case JoinMode::Semi:
        case JoinMode::AntiNullAsTrue:
        case JoinMode::AntiNullAsFalse:
          if (next_input_side == LQPInputSide::Right) {
            auto next_column_blacklist = get_column_references(*join_node->input(LQPInputSide::Left));
            traverse(input, next_input_side, visited, next_column_blacklist, equal_subplans, subplan_mapping);
            break;
          } else {
            [[fallthrough]];
          }

        case JoinMode::Inner:
        case JoinMode::Left:
        case JoinMode::Right:
        case JoinMode::FullOuter:
        case JoinMode::Cross: {
          auto next_column_blacklist = column_blacklist;
          const auto opposite_input_column_references =
              get_column_references(*join_node->input(opposite_input_side));
          next_column_blacklist.insert(opposite_input_column_references.begin(), opposite_input_column_references.end());
          traverse(input, next_input_side, visited, next_column_blacklist, equal_subplans, subplan_mapping);
        } break;
      }
    }
  } else {
    traverse(input, LQPInputSide::Left, visited, column_blacklist, equal_subplans, subplan_mapping);
    traverse(input, LQPInputSide::Right, visited, column_blacklist, equal_subplans, subplan_mapping);
  }
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
      equal_subplans.emplace(subplan, SubplanEqualityMapping::mapped_type{});
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
  const auto column_blacklist = get_column_references(*root);
  auto subplan_replacements = SubplanReplacementMapping{};
  traverse(root, LQPInputSide::Left, visited_nodes, column_blacklist, equal_subplans, subplan_replacements);

  if (subplan_replacements.empty()) {
    return;
  }

  std::cout << "SubplanReuseRule root\n" << *root << std::endl;

  /**
   * Replace subplans
   */
  auto column_replacements = ColumnReplacementMapping{};
  for (const auto& [parent, input_side, replacement_plan] : subplan_replacements) {
    const auto obsolete_plan = parent->input(input_side);
    add_column_replacements(column_replacements, *obsolete_plan, *replacement_plan);

    std::cout << "SubplanReuseRule: Replacing\n" << *parent->input(input_side) << "\nwith\n" << *replacement_plan << std::endl;

    parent->set_input(input_side, replacement_plan);
  }

  std::cout << "ColumnReplacements: \n";

  for (auto [from, to] : column_replacements) {
    std::cout << " " << from << " -> " << to << std::endl;
  }

  /**
   * Fix all expressions that referred to now-removed subplans
   */
  visit_lqp(root, [&](const auto& node) {
    for(auto& node_expression : node->node_expressions) {
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
