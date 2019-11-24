#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "operators/operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode) : AbstractLQPNode(LQPNodeType::Join), join_mode(join_mode) {
  Assert(join_mode == JoinMode::Cross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate)
    : JoinNode(join_mode, std::vector<std::shared_ptr<AbstractExpression>>{join_predicate}) {}

JoinNode::JoinNode(const JoinMode join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates)
    : AbstractLQPNode(LQPNodeType::Join, join_predicates), join_mode(join_mode) {
  Assert(join_mode != JoinMode::Cross, "Cross Joins take no predicate");
  Assert(!join_predicates.empty(), "Non-Cross Joins require predicates");
}

std::string JoinNode::description() const {
  std::stringstream stream;
  stream << "[Join] Mode: " << join_mode;

  for (const auto& predicate : join_predicates()) {
    stream << " [" << predicate->as_column_name() << "]";
  }

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::column_expressions() const {
  Assert(left_input() && right_input(), "Both inputs need to be set to determine a JoiNode's output expressions");

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = left_input()->column_expressions();
  const auto& right_expressions = right_input()->column_expressions();

  // TODO assert that we do not emit duplicates here

  const auto output_both_inputs =
      join_mode != JoinMode::Semi && join_mode != JoinMode::AntiNullAsTrue && join_mode != JoinMode::AntiNullAsFalse;

  if (!output_both_inputs) {
    return left_expressions;
  }

  _column_expressions.resize(left_expressions.size() + right_expressions.size());

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), _column_expressions.begin());
  std::copy(right_expressions.begin(), right_expressions.end(), right_begin);

  const auto collect_column_references =
      [](const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {  // TODO dedup
        auto column_references = std::unordered_set<std::reference_wrapper<const LQPColumnReference>,
                                                    std::hash<const opossum::LQPColumnReference>,
                                                    std::equal_to<const opossum::LQPColumnReference>>{};
        column_references.reserve(expressions.size() * 10);
        for (const auto& expression : expressions) {
          {
            // TODO test if shortcut is good for performance
            if (const auto column_expression = dynamic_cast<const LQPColumnExpression*>(&*expression)) {
              if (!column_references.contains(std::cref(column_expression->column_reference))) {
                column_references.emplace(std::cref(column_expression->column_reference));
              }
              continue;
            }
          }

          const auto lambda = [&](const auto& sub_expression) {
            if (const auto column_expression = dynamic_cast<const LQPColumnExpression*>(&*sub_expression)) {
              if (!column_references.contains(std::cref(column_expression->column_reference))) {
                column_references.emplace(std::cref(column_expression->column_reference));
              }
              return ExpressionVisitation::DoNotVisitArguments;
            }
            return ExpressionVisitation::VisitArguments;
          };
          visit_expression<std::remove_reference_t<decltype(expression)>, decltype(lambda), 1>(expression, lambda);
        }
        return column_references;
      };

  const auto left_column_references = collect_column_references(left_expressions);
  const auto right_column_references = collect_column_references(right_expressions);

  auto ambiguous_references = std::unordered_set<LQPColumnReference>{};
  for (const auto& left_column_reference : left_column_references) {
    // TODO optimize this
    if (right_column_references.contains(left_column_reference)) {
      ambiguous_references.emplace(left_column_reference);
    }
  }

  if (ambiguous_references.empty()) {
    return _column_expressions;
  }

  for (auto left_expression_iter = _column_expressions.begin();
       left_expression_iter != _column_expressions.begin() + left_expressions.size(); ++left_expression_iter) {
    auto expression_copy = (*left_expression_iter)->deep_copy();
    auto replacement_occured = false;
    const auto lambda = [&](auto& sub_expression) {
      if (const auto column_expression = dynamic_cast<LQPColumnExpression*>(&*sub_expression)) {
        if (!ambiguous_references.contains(column_expression->column_reference))
          return ExpressionVisitation::DoNotVisitArguments;

        auto disambiguated_column_reference = column_expression->column_reference;
        disambiguated_column_reference.lineage.emplace_back(shared_from_this(), LQPInputSide::Left);
        auto disambiguated_expression = std::make_shared<LQPColumnExpression>(disambiguated_column_reference);
        sub_expression = disambiguated_expression;

        replacement_occured = true;
      }
      return ExpressionVisitation::VisitArguments;
    };
    visit_expression<std::remove_reference_t<decltype(expression_copy)>, decltype(lambda), 2>(expression_copy, lambda);
    if (replacement_occured) *left_expression_iter = expression_copy;
  }

  for (auto right_expression_iter = _column_expressions.begin() + left_expressions.size();
       right_expression_iter != _column_expressions.end(); ++right_expression_iter) {
    auto expression_copy = (*right_expression_iter)->deep_copy();
    auto replacement_occured = false;
    const auto lambda = [&](auto& sub_expression) {
      if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
        if (!ambiguous_references.contains(column_expression->column_reference))
          return ExpressionVisitation::DoNotVisitArguments;

        auto disambiguated_column_reference = column_expression->column_reference;
        disambiguated_column_reference.lineage.emplace_back(shared_from_this(), LQPInputSide::Right);
        auto disambiguated_expression = std::make_shared<LQPColumnExpression>(disambiguated_column_reference);
        sub_expression = disambiguated_expression;

        replacement_occured = true;
      }
      return ExpressionVisitation::VisitArguments;
    };
    visit_expression<std::remove_reference_t<decltype(expression_copy)>, decltype(lambda), 3>(expression_copy, lambda);
    if (replacement_occured) *right_expression_iter = expression_copy;
  }

  return _column_expressions;
}

bool JoinNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  const auto left_input_column_count = left_input()->column_expressions().size();
  const auto column_is_from_left_input = column_id < left_input_column_count;

  if (join_mode == JoinMode::Left && !column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::Right && column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::FullOuter) {
    return true;
  }

  if (column_is_from_left_input) {
    return left_input()->is_column_nullable(column_id);
  } else {
    ColumnID right_column_id =
        static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
    return right_input()->is_column_nullable(right_column_id);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const { return node_expressions; }

std::optional<ColumnID> JoinNode::find_column_id(const AbstractExpression& expression) const {
  std::optional<ColumnID> column_id_on_left;
  std::optional<ColumnID> column_id_on_right;

  // We might need to disambiguate the expression using the lineage information in the LQPColumnReferences.
  std::optional<LQPInputSide> disambiguated_input_side;
  auto disambiguated_expression = expression.deep_copy();

  visit_expression(disambiguated_expression, [&](auto& sub_expression) {
    if (const auto column_expression = dynamic_cast<LQPColumnExpression*>(&*sub_expression)) {
      auto& column_reference = column_expression->column_reference;
      if (column_reference.lineage.empty()) return ExpressionVisitation::VisitArguments;

      const auto last_lineage_step = column_reference.lineage.back();
      if (&*last_lineage_step.first.lock() != this) return ExpressionVisitation::VisitArguments;

      if (disambiguated_input_side && *disambiguated_input_side == last_lineage_step.second) {
        // failed resolving
        return ExpressionVisitation::DoNotVisitArguments;
      }

      disambiguated_input_side = last_lineage_step.second;

      // Remove this node from the lineage information of sub_expression
      column_reference.lineage.pop_back();

      // visit_expression ends here
    }
    return ExpressionVisitation::VisitArguments;
  });

  const auto left_input_column_count = left_input()->column_expressions().size();
  const auto& this_column_expressions = column_expressions();
  for (auto column_id = ColumnID{0}; column_id < this_column_expressions.size(); ++column_id) {
    // TODO can we do the first part earlier?
    if (*this_column_expressions[column_id] != expression &&
        *this_column_expressions[column_id] != *disambiguated_expression)
      continue;
    if (column_id < left_input_column_count) {
      column_id_on_left = column_id;
    } else {
      column_id_on_right = column_id;
    }
  }

  if (disambiguated_input_side) {
    if (column_id_on_left) {
      DebugAssert(*disambiguated_input_side == LQPInputSide::Left, "Expression has lineage information pointing to left side, but could not be resolved there");
    }
    if (column_id_on_right) {
      DebugAssert(*disambiguated_input_side == LQPInputSide::Right, "Expression has lineage information pointing to right side, but could not be resolved there");
    }
  }

  if (column_id_on_left && !column_id_on_right) {
    // Found unambiguously on left side
    return column_id_on_left;
  }

  if (column_id_on_right && !column_id_on_left) {
    // Found unambiguously on right side
    return column_id_on_right;
  }

  return std::nullopt;
}

//   // TODO pseudo-ambiguity could also come from literal columns on both sides. Test that those also work

size_t JoinNode::_shallow_hash() const { return boost::hash_value(join_mode); }

std::shared_ptr<AbstractLQPNode> JoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (!join_predicates().empty()) {
    const auto copy = JoinNode::make(join_mode, join_predicates());

    // Join predicates may self-reference the join as part of their LQPColumnReference's lineage. We need to first
    // create the JoinNode so that we have its address, then we can update the lineage information.
    node_mapping.emplace(shared_from_this(), copy);
    for (auto& join_predicate : copy->node_expressions) {
      join_predicate = expression_copy_and_adapt_to_different_lqp(*join_predicate, node_mapping);
    }
    return copy;
  } else {
    return JoinNode::make(join_mode);
  }
}

bool JoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode) return false;
  return expressions_equal_to_expressions_in_different_lqp(join_predicates(), join_node.join_predicates(),
                                                           node_mapping);
}

}  // namespace opossum
