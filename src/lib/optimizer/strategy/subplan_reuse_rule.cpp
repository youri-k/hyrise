#include "subplan_reuse_rule.hpp"

#include <unordered_map>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT

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

void add_to_column_mapping(const std::shared_ptr<AbstractExpression>& from_expression,
                           const std::shared_ptr<AbstractExpression>& to_expression,
                           ColumnReplacementMappings& mappings) {
  Assert(from_expression->type == to_expression->type, "Expected same type");

  std::cout << "add_to_column_mapping(" << from_expression->description(AbstractExpression::DescriptionMode::Detailed) << ", " << to_expression->description(AbstractExpression::DescriptionMode::Detailed) << ")\n";

  // TODO test that LQPColumnExpressions hidden in an argument (e.g., SUM(x) or AND(x, y)) are also detected
  // TODO test COUNT(*), also nested and some LQP nodes away

  if (const auto from_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(from_expression)) {
    const auto to_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(to_expression);
    mappings.emplace(from_column_expression->column_reference, to_column_expression->column_reference);
  } else {
    auto from_expressions_arguments_iter = from_expression->arguments.begin();
    auto from_expressions_arguments_end_iter = from_expression->arguments.end();
    auto to_expressions_arguments_iter = to_expression->arguments.begin();

    DebugAssert(from_expressions_arguments_end_iter - from_expressions_arguments_iter ==
                    to_expression->arguments.end() - to_expressions_arguments_iter,
                "Mismatching number of expression arguments");

    while (from_expressions_arguments_iter != from_expressions_arguments_end_iter) {
      add_to_column_mapping(*from_expressions_arguments_iter, *to_expressions_arguments_iter, mappings);
      ++from_expressions_arguments_iter;
      ++to_expressions_arguments_iter;
    }
  }
}

ColumnReplacementMappings create_column_mapping(const std::shared_ptr<AbstractLQPNode>& from_node, const std::shared_ptr<AbstractLQPNode>& to_node) {
  auto mapping = std::unordered_map<LQPColumnReference, LQPColumnReference>{};

  const auto& from_expressions = from_node->column_expressions();
  const auto& to_expressions = to_node->column_expressions();

  Assert(from_expressions.size() == to_expressions.size(), "Expected same number of expressions");

  for (auto column_id = ColumnID{0}; column_id < from_expressions.size(); ++column_id) {
    add_to_column_mapping(from_expressions[column_id], to_expressions[column_id], mapping);
  }

  // Add COUNT(*)s
  for (const auto& [from, to] : mapping) {
    auto adapted_from = LQPColumnReference(from.original_node(), INVALID_COLUMN_ID);
    adapted_from.lineage = from.lineage;

    auto adapted_to = LQPColumnReference(to.original_node(), INVALID_COLUMN_ID);
    adapted_to.lineage = to.lineage;

    mapping.emplace(adapted_from, adapted_to);
  }

  return mapping;
}

// TODO test multiple self joins to force multi-step lineage - e.g., SELECT * FROM (SELECT t1.id FROM id_int_int_int_100 t1 JOIN id_int_int_int_100 t2 ON t1.id + 1 = t2.id) AS s1, id_int_int_int_100 t3 WHERE s1.id + 5 = t3.id

void apply_column_replacement_mappings(std::shared_ptr<AbstractExpression>& expression,
                                       const ColumnReplacementMappings& column_replacement_mappings) {
  // need copy so that we do not manipulate upstream expressions
  auto expression_copy = expression->deep_copy();
  auto replacement_occured = false;
  visit_expression(expression_copy, [&column_replacement_mappings, &replacement_occured](auto& sub_expression) {
    if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
      auto reduced_column_reference = column_expression->column_reference;
      auto upstream_lineage = std::vector<std::pair<std::weak_ptr<const AbstractLQPNode>, LQPInputSide>>{};
      auto new_column_reference = LQPColumnReference{};

      auto try_again = false;
      do {
        try_again = false;
        std::cout << "\tshould I replace " << reduced_column_reference.description() << "?" << std::endl;
        const auto column_replacement_iter = column_replacement_mappings.find(reduced_column_reference);
        if (column_replacement_iter != column_replacement_mappings.end()) {
          new_column_reference = column_replacement_iter->second;


          std::cout << "replaced with " << new_column_reference.description() << std::endl;

          replacement_occured = true;
        } else {
          // if (!reduced_column_reference.lineage.empty()) {
          //   upstream_lineage.emplace_back(reduced_column_reference.lineage.back());
          //   reduced_column_reference.lineage.pop_back();
          //   try_again = true;
          // }
        }
      } while (try_again);

      if (replacement_occured) {
        // for (auto lineage_iter = upstream_lineage.rbegin(); lineage_iter != upstream_lineage.rend(); ++lineage_iter) {
        //   new_column_reference.lineage.emplace_back(*lineage_iter);
        // }
        // std::cout << "but restored lineage as " << new_column_reference.description() << std::endl;
        sub_expression = std::make_shared<LQPColumnExpression>(new_column_reference);
      }

      // TheLQPColumnExpression has no inputs, so at this point, we have reached the end of visit_expression.
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

// TODO rename to collect or alike?
void apply_column_replacement_mappings_upwards(
    const std::shared_ptr<AbstractLQPNode>& node, ColumnReplacementMappings& column_replacement_mappings,
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, ColumnReplacementMappings>& per_node_replacements) {
  std::cout << "!!!apply_column_replacement_mappings_upwards!!!" << std::endl;
  visit_lqp_upwards(node, [&column_replacement_mappings, &per_node_replacements](const auto& sub_node) {
    std::cout << *sub_node << std::endl;
    auto column_replacement_mappings_local = column_replacement_mappings;

    if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(sub_node)) {
      const auto left_column_references = get_column_references(join_node->left_input()->column_expressions());
      const auto right_column_references = get_column_references(join_node->right_input()->column_expressions());

      // Cannot change column_replacement_mappings during iteration
      ColumnReplacementMappings updated_mappings;

      for (auto& [from, to] : column_replacement_mappings) {
//         if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue &&
//             join_node->join_mode != JoinMode::AntiNullAsFalse) {
//           DebugAssert(!left_column_references.contains(from) || !right_column_references.contains(from),
//                       "Ambiguous mapping 1");
//         for (const auto& x : join_node->column_expressions()) std::cout << "\t" << *x << std::endl;
        std::cout << "old: " << from.description() << " => " << to.description() << std::endl;


// // Looks like the new mapping is not properly passed upwards



//           DebugAssert(!left_column_references.contains(to) || !right_column_references.contains(to),
//                       "Ambiguous mapping 2");
//         }

        if (left_column_references.contains(from) && right_column_references.contains(to)) {
          auto updated_to_left = to;
          updated_to_left.lineage.emplace_back(join_node->shared_from_this(), LQPInputSide::Left);
          updated_mappings[from] = updated_to_left;

          auto updated_to_right = to;
          updated_to_right.lineage.emplace_back(join_node->shared_from_this(), LQPInputSide::Right);
          updated_mappings[to] = updated_to_right;
        }

        if (right_column_references.contains(from) && left_column_references.contains(to)) {
          auto updated_to_right = to;
          updated_to_right.lineage.emplace_back(join_node->shared_from_this(), LQPInputSide::Right);
          updated_mappings[from] = updated_to_right;

          auto updated_to_left = to;
          updated_to_left.lineage.emplace_back(join_node->shared_from_this(), LQPInputSide::Left);
          updated_mappings[to] = updated_to_left;
        }
      }

      for (const auto& [from, to] : updated_mappings) {
        column_replacement_mappings_local[from] = to;
        if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue &&
            join_node->join_mode != JoinMode::AntiNullAsFalse) {
          column_replacement_mappings[from] = to;
        }
      }



      for (const auto& [from, to] : column_replacement_mappings) {
        std::cout << "tmp: " << from.description() << " => " << to.description() << std::endl;
      }

// TODO add << for LQPInputSide

      updated_mappings.clear();
      if (join_node->join_mode != JoinMode::Semi && join_node->join_mode != JoinMode::AntiNullAsTrue &&
            join_node->join_mode != JoinMode::AntiNullAsFalse) {
        for (const auto& [from, to] : column_replacement_mappings) {
          if (!to.lineage.empty() && to.lineage.back().first.lock() == join_node) continue;
          // Someone might be using this node as part of their lineage - update those as well
          for (const auto side : {LQPInputSide::Left, LQPInputSide::Right}) {
            std::cout << "testing: " << from.description() << " => " << to.description() << " on " << (side == LQPInputSide::Left ? "left" : "right") << std::endl;
            bool all_via_nodes_found = true;
            for (const auto& via : to.lineage) {
              std::cout << "\tvia " << via.first.lock() << std::endl;
              bool node_found = false;
              visit_lqp(join_node->input(side), [&](const auto x) {
                if (x == via.first.lock()) {
                  std::cout << "\t\tfound" << std::endl;
                  node_found = true;
                }
                return LQPVisitation::VisitInputs;
              });
              if (!node_found) all_via_nodes_found = false;
            }
            if (!all_via_nodes_found) continue;

            auto updated_from = from;
            updated_from.lineage.emplace_back(join_node, side);
            auto updated_to = to;
            updated_to.lineage.emplace_back(join_node, side);
            updated_mappings[updated_from] = updated_to;
          }
        }
      }

      for (const auto& [from, to] : updated_mappings) {
        column_replacement_mappings_local[from] = to;
        column_replacement_mappings[from] = to;
      }






      for (const auto& [from, to] : column_replacement_mappings) {
        std::cout << "new: " << from.description() << " => " << to.description() << std::endl;
      }
    }

    per_node_replacements[sub_node] = column_replacement_mappings_local;
    return LQPUpwardVisitation::VisitOutputs;
  });
}

}  // namespace

// TODO Test the things from the whiteboard

namespace opossum {

// TODO Is it necessary to update the lineage? Test this

void SubplanReuseRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "SubplanReuseRule needs root to hold onto");

  std::cout << "\n\n\n=== IN ===" << std::endl;
  std::cout << *root << std::endl;

  bool more = true;  // TODO: try_another_iteration
  while (more) {
    LQPNodeUnorderedSet primary_subplans;
    more = false;
    visit_lqp(root, [&](const auto& node) {
      if (more) return LQPVisitation::DoNotVisitInputs;
      const auto [primary_subplan_iter, is_primary_subplan] = primary_subplans.emplace(node);
      if (is_primary_subplan) return LQPVisitation::VisitInputs;

      // We have seen this plan before and can reuse it
      const auto& primary_subplan = *primary_subplan_iter;

      auto column_mapping = create_column_mapping(node, primary_subplan);

      std::unordered_map<std::shared_ptr<AbstractLQPNode>, ColumnReplacementMappings>
          per_node_replacements;  // TODO does this have to be a map?

      apply_column_replacement_mappings_upwards(node, column_mapping, per_node_replacements);

      for (const auto& [node, mapping] : per_node_replacements) {
        // std::cout << node->description() << std::endl;
        apply_column_replacement_mappings(node->node_expressions, mapping);
      }

      for (const auto& [output, input_side] : node->output_relations()) {
        output->set_input(input_side, primary_subplan);
      }

      std::cout << "\n\n\n=== TMP ===" << std::endl;
      std::cout << *root << std::endl;

      // TODO
      more = true;

      return LQPVisitation::DoNotVisitInputs;
    });
  }

  std::cout << "\n\n\n=== OUT ===" << std::endl;
  std::cout << *root << std::endl;
}

}  // namespace opossum
