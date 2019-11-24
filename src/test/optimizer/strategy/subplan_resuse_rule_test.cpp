#include "gtest/gtest.h"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/strategy/subplan_reuse_rule.hpp"
#include "strategy_base_test.hpp"

#include "testing_assert.hpp"

namespace opossum {

class SubplanReuseRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    mock_node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");

    a_a = mock_node_a->get_column("a");
    b_a = mock_node_b->get_column("a");
    c_a = mock_node_c->get_column("a");

    rule = std::make_shared<SubplanReuseRule>();
  }

  std::shared_ptr<MockNode> mock_node_a, mock_node_b, mock_node_c;
  LQPColumnReference a_a, b_a, c_a;
  std::shared_ptr<SubplanReuseRule> rule;
};

TEST_F(SubplanReuseRuleTest, BasicReuseMadePossibleByAggregate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(sum_(a_a), 10),
    JoinNode::make(JoinMode::Semi, equals_(sum_(a_a), b_a),
      AggregateNode::make(expression_vector(), expression_vector(sum_(a_a)),
        mock_node_a),
      mock_node_b));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(sum_(b_a), 10),
    JoinNode::make(JoinMode::Semi, equals_(sum_(b_a), b_a),
      AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
        mock_node_b),
      mock_node_b));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubplanReuseRuleTest, BasicReuseMadePossibleBySemiJoin) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 10),
    JoinNode::make(JoinMode::Semi, equals_(a_a, b_a),
      mock_node_a,
      JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
        mock_node_b,
        mock_node_c)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(c_a, 10),
    JoinNode::make(JoinMode::Semi, equals_(c_a, b_a),
      mock_node_c,
      JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
        mock_node_b,
        mock_node_c)));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubplanReuseRuleTest, NoReuse) {
  // clang-format off
  const auto input_lqp_a =
  JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
    mock_node_b,
    mock_node_c);

  const auto input_lqp_b =
    JoinNode::make(JoinMode::Semi, equals_(b_a, c_a),
       mock_node_b,
       mock_node_c);

  const auto input_lqp_c =
  JoinNode::make(JoinMode::Cross,
    mock_node_a,
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      mock_node_b,
      mock_node_c));

  const auto input_lqp_d =
  PredicateNode::make(greater_than_(a_a, 10),
    JoinNode::make(JoinMode::Inner, equals_(sum_(a_a), b_a),
      AggregateNode::make(expression_vector(), expression_vector(sum_(a_a)),
        mock_node_a),
      mock_node_b));

  const auto expected_lqp_a = input_lqp_a->deep_copy();
  const auto expected_lqp_b = input_lqp_b->deep_copy();
  const auto expected_lqp_c = input_lqp_c->deep_copy();
  const auto expected_lqp_d = input_lqp_d->deep_copy();
  // clang-format on

  const auto actual_lqp_a = apply_rule(rule, input_lqp_a);
  const auto actual_lqp_b = apply_rule(rule, input_lqp_b);
  const auto actual_lqp_c = apply_rule(rule, input_lqp_c);
  const auto actual_lqp_d = apply_rule(rule, input_lqp_d);

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
}

}  // namespace opossum
