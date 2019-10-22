#include "subplan_reuse_rule.hpp"

#include <unordered_map>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

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

using ColumnReplacementMappings = std::unordered_map<LQPColumnReference, LQPColumnReference>;
ColumnReplacementMappings create_column_mapping(const AbstractLQPNode& from_node, const AbstractLQPNode& to_node) {
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

void apply_column_replacement_mappings(std::shared_ptr<AbstractExpression>& expression,
                                      const ColumnReplacementMappings& column_replacement_mappings) {

  for (const auto& [from, to] : column_replacement_mappings) {
    // Not sure if this is a valid assertion. In any case, lineage on the `from` side is unhandled.
    DebugAssert(from.lineage.empty(), "Expected lineage of `from` side to be empty");
  }

  // need copy so that we do not manipulate upstream expressions
  auto expression_copy = expression->deep_copy();
  auto replacement_occured = false;
  visit_expression(expression_copy, [&column_replacement_mappings, &replacement_occured](auto& sub_expression) {
    if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
      auto column_reference_without_lineage = LQPColumnReference{column_expression->column_reference.original_node(), column_expression->column_reference.original_column_id()};
      const auto column_replacement_iter = column_replacement_mappings.find(column_reference_without_lineage);
      if (column_replacement_iter != column_replacement_mappings.end()) {
        auto new_column_reference = column_replacement_iter->second;
        // Restore lineage
        for (const auto& via : column_expression->column_reference.lineage) {
          new_column_reference.lineage.emplace(via);
        }

        sub_expression = std::make_shared<LQPColumnExpression>(new_column_reference);

        replacement_occured = true;
      }
    }
    return ExpressionVisitation::VisitArguments;
  });
  if (replacement_occured) expression = expression_copy;
}

void apply_column_replacement_mappings(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                      const ColumnReplacementMappings& column_replacement_mappings) {
  for (auto& expression : expressions) {
    apply_column_replacement_mappings(expression, column_replacement_mappings);
  }
}

void apply_column_replacement_mappings_upwards(const std::shared_ptr<AbstractLQPNode>& node,
                                      ColumnReplacementMappings& column_replacement_mappings,
                                      std::unordered_map<std::shared_ptr<AbstractLQPNode>, ColumnReplacementMappings>& per_node_replacements) {
  visit_lqp_upwards(node, [&column_replacement_mappings, &per_node_replacements](const auto& sub_node) {

    auto column_replacement_mappings_local = column_replacement_mappings;


    if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(sub_node)) {
      const auto left_column_references = get_column_references(join_node->left_input()->column_expressions());
      const auto right_column_references = get_column_references(join_node->right_input()->column_expressions());


      ColumnReplacementMappings updated_mappings;

      for (auto& [from, to] : column_replacement_mappings) {
        if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse) {
          DebugAssert(!left_column_references.contains(from) || !right_column_references.contains(from), "Ambiguous mapping 1");
          DebugAssert(!left_column_references.contains(to) || !right_column_references.contains(to), "Ambiguous mapping 2");
        }


        if (left_column_references.contains(from) && right_column_references.contains(to)) {
          auto updated_to_left = to;
          updated_to_left.lineage.emplace(join_node->shared_from_this(), LQPInputSide::Left);
          updated_mappings[from] = updated_to_left;

          auto updated_to_right = to;
          updated_to_right.lineage.emplace(join_node->shared_from_this(), LQPInputSide::Right);
          updated_mappings[to] = updated_to_right;

        }

        if (right_column_references.contains(from) && left_column_references.contains(to)) {
          auto updated_to_right = to;
          updated_to_right.lineage.emplace(join_node->shared_from_this(), LQPInputSide::Right);
          updated_mappings[from] = updated_to_right;

          auto updated_to_left = to;
          updated_to_left.lineage.emplace(join_node->shared_from_this(), LQPInputSide::Left);
          updated_mappings[to] = updated_to_left;

        }
      }

      // TODO: Test multiple joins where only an upper join disambiguates

      // Problem: "ps_supplycost from 0x11ff43dd8 via 0x120ebc258(right)" wird angeblich von der rechten Seite eines Semi-Joins bedient
      // 1. OperatorJoinPredicate muss lernen, selbst lineage für den untersuchten Join rauszunehmen / oooder find_column_id anpassen/überschreiben
      // 2. Für Semi/Anti-Joins dürfen hier nur die join_predicates umgeschrieben werden.
      //   - Das Mapping darf nicht geupdated werden
      //   - Der JoinNode darf keine Lineage hinzufügen




      // auto join_column_references = left_column_references;
      // auto right_column_references_copy = right_column_references;
      // join_column_references.merge(right_column_references_copy);


      // // TODO Rename join_predicate here
      // for (const auto& join_predicate : join_node->column_expressions()) {  // TODO do we really need this loop or can we iterate over the mappings
      //   visit_expression(join_predicate, [&](const auto& sub_expression) {
      //     if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {

      //       const auto& local = column_expression->column_reference;

      //       const auto comes_from_left = left_column_references.contains(local);
      //       const auto comes_from_right = right_column_references.contains(local);
      //       DebugAssert(comes_from_left != comes_from_right, "Expected LQPColumnExpression to come from exactly one join side");
      //       const auto side = comes_from_left ? LQPInputSide::Left : LQPInputSide::Right;


      //       for (auto& [from, to] : column_replacement_mappings) {
      //         if (!join_column_references.contains(to)) {
      //           continue;
      //         }

      //         DebugAssert(from != to, "Invalid mapping");

      //         auto updated_to = to;
      //         updated_to.lineage.emplace(join_node, side);

      //         if (local == from) {
      //           updated_mappings.emplace(from, updated_to);
      //           continue;
      //         }

      //         if (local == to) {
      //           updated_mappings.emplace(to, updated_to);
      //         }
      //       }

      //     }
      //     return ExpressionVisitation::VisitArguments;
      //   });
      // }

      for (const auto& [from, to] : updated_mappings) {
        column_replacement_mappings_local[from] = to;
        if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse) {
          column_replacement_mappings[from] = to;
        }
      }
    }

    per_node_replacements[sub_node] = column_replacement_mappings_local;
    return LQPUpwardVisitation::VisitOutputs;
  });
}

}  // namespace

// TODO Test the things from the whiteboard

namespace opossum {

using LQPNodeUnorderedSet =  // TODO move to abstract_lqp_node.hpp
    std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual>;  

// TODO Is it necessary to update the lineage? Test this

void SubplanReuseRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "SubplanReuseRule needs root to hold onto");

  bool more = true;
  while (more) {
    LQPNodeUnorderedSet primary_subplans;
    more = false;
    visit_lqp(root, [&](const auto& node) {
      if (more) return LQPVisitation::DoNotVisitInputs;
      const auto [primary_subplan_iter, is_primary_subplan] = primary_subplans.emplace(node);
      if (is_primary_subplan) return LQPVisitation::VisitInputs;

      // We have seen this plan before and can reuse it
      const auto& primary_subplan = *primary_subplan_iter;

      auto column_mapping = create_column_mapping(*node, *primary_subplan);
      std::unordered_map<std::shared_ptr<AbstractLQPNode>, ColumnReplacementMappings> per_node_replacements;  // TODO does this have to be a map?

      apply_column_replacement_mappings_upwards(node, column_mapping, per_node_replacements);

      for (const auto& [node, mapping] : per_node_replacements) {
        apply_column_replacement_mappings(node->node_expressions, mapping);
      }

      for (const auto& [output, input_side] : node->output_relations()) {
        output->set_input(input_side, primary_subplan);
      }

      more = true;

      return LQPVisitation::DoNotVisitInputs;
    });
  }
}

}  // namespace opossum
