#include "operator_join_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

namespace opossum {

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& left_input,
                                                                            const AbstractLQPNode& right_input) {  // TODO is this still used?
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) return std::nullopt;

  switch (abstract_predicate_expression->predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }

  Assert(abstract_predicate_expression->arguments.size() == 2u, "Expected two arguments");

  const auto left_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto left_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto right_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[1]);
  const auto right_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[1]);

  auto predicate_condition = abstract_predicate_expression->predicate_condition;

  if (left_in_left && right_in_right) {
    return OperatorJoinPredicate{{*left_in_left, *right_in_right}, predicate_condition};
  }

  if (right_in_left && left_in_right) {
    predicate_condition = flip_predicate_condition(predicate_condition);
    return OperatorJoinPredicate{{*right_in_left, *left_in_right}, predicate_condition};
  }

  return std::nullopt;
}

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& join_node) {
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) return std::nullopt;

  switch (abstract_predicate_expression->predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }

  Assert(abstract_predicate_expression->arguments.size() == 2u, "Expected two arguments");

  // Overwrite mode so that find_column_id is not left with only the left side
  // TODO do this somehow differently

  auto& casted_join_node = const_cast<JoinNode&>(static_cast<const JoinNode&>(join_node));
  const auto old_mode = casted_join_node.join_mode;
  casted_join_node.join_mode = JoinMode::Inner;
  auto left_arg_column_id = casted_join_node.find_column_id(*abstract_predicate_expression->arguments[0]);
  auto right_arg_column_id = casted_join_node.find_column_id(*abstract_predicate_expression->arguments[1]);
  casted_join_node.join_mode = old_mode;

  if (!left_arg_column_id || !right_arg_column_id) return std::nullopt;

  auto predicate_condition = abstract_predicate_expression->predicate_condition;


  const auto num_left_column_expressions =
      static_cast<ColumnID::base_type>(join_node.left_input()->column_expressions().size());

  DebugAssert(*left_arg_column_id >= num_left_column_expressions || *right_arg_column_id >= num_left_column_expressions, "Join arguments are not unambiguously from left or right");

  if (*left_arg_column_id < *right_arg_column_id) {
    return OperatorJoinPredicate{
        {*left_arg_column_id,
         ColumnID{static_cast<ColumnID::base_type>(*right_arg_column_id - num_left_column_expressions)}},
        predicate_condition};
  } else {
    predicate_condition = flip_predicate_condition(predicate_condition);
    return OperatorJoinPredicate{
        {*right_arg_column_id,
         ColumnID{static_cast<ColumnID::base_type>(*left_arg_column_id - num_left_column_expressions)}},
        predicate_condition};
  }
}

OperatorJoinPredicate::OperatorJoinPredicate(const ColumnIDPair& column_ids,
                                             const PredicateCondition predicate_condition)
    : column_ids(column_ids), predicate_condition(predicate_condition) {}

void OperatorJoinPredicate::flip() {
  std::swap(column_ids.first, column_ids.second);
  predicate_condition = flip_predicate_condition(predicate_condition);
}

bool operator<(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) < std::tie(r.column_ids, r.predicate_condition);
}

bool operator==(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) == std::tie(r.column_ids, r.predicate_condition);
}

}  // namespace opossum
