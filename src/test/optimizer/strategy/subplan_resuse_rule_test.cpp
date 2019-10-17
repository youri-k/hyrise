#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/strategy/subplan_reuse_rule.hpp"

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

TEST_F(SubplanReuseRuleTest, Basic) {
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
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    mock_node_a,
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      mock_node_b,
      mock_node_c));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    mock_node_a,
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      mock_node_b,
      mock_node_c));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
