#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/rules/predicate_reordering_rule.hpp"
#include "optimizer/rules/rule_base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateReorderingRuleTest : public RuleBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_int_int.tbl"));
    StorageManager::get().add_table("b", load_table("src/test/tables/int_float4.tbl"));
    
    _rule = std::make_shared<PredicateReorderingRule>();

    std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics_a(
        {std::make_shared<ColumnStatistics<int32_t>>(0.0f, 20, 10, 100),
         std::make_shared<ColumnStatistics<int32_t>>(0.0f, 5, 50, 60),
         std::make_shared<ColumnStatistics<int32_t>>(0.0f, 2, 110, 1100)});

    auto table_statistics_a = std::make_shared<TableStatistics>(TableType::Data, 100, column_statistics_a);

    node_a = StoredTableNode::make("a");
    StorageManager::get().get_table("a")->set_table_statistics(table_statistics_a);

    std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics_b(
    {std::make_shared<ColumnStatistics<int32_t>>(0.0f, 20, 10, 100),
     std::make_shared<ColumnStatistics<float>>(0.0f, 5, 50, 60)});

    auto table_statistics_b = std::make_shared<TableStatistics>(TableType::Data, 100, column_statistics_b);

    node_b = StoredTableNode::make("b");
    StorageManager::get().get_table("b")->set_table_statistics(table_statistics_b);
    
    a_a = LQPColumnReference{node_a, ColumnID{0}};
    a_b = LQPColumnReference{node_a, ColumnID{1}};
    a_c = LQPColumnReference{node_a, ColumnID{2}};
    b_a = LQPColumnReference{node_b, ColumnID{0}};
    b_b = LQPColumnReference{node_b, ColumnID{1}};
  }

  std::shared_ptr<StoredTableNode> node_a, node_b;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b;
  std::shared_ptr<PredicateReorderingRule> _rule;
};

TEST_F(PredicateReorderingRuleTest, SimpleReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    PredicateNode::make(greater_than_(a_a, 10),
      node_a));
  const auto expected_lqp =
  PredicateNode::make(greater_than_(a_a, 10),
    PredicateNode::make(greater_than_(a_a, 50),
      node_a));
  // clang-format on

  const auto reordered_input_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

TEST_F(PredicateReorderingRuleTest, MoreComplexReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 99),
    PredicateNode::make(greater_than_(a_b, 55),
      PredicateNode::make(greater_than_(a_c, 100),
        node_a)));
  const auto expected_lqp =
  PredicateNode::make(greater_than_(a_c, 100),
    PredicateNode::make(greater_than_(a_b, 55),
      PredicateNode::make(greater_than_(a_a, 99),
        node_a)));
  // clang-format on

  const auto reordered_input_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

TEST_F(PredicateReorderingRuleTest, ComplexReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a_a, 42),
    PredicateNode::make(greater_than_(a_b, 50),
      PredicateNode::make(greater_than_(a_b, 40),
        ProjectionNode::make(expression_vector(a_a, a_b, a_c),
          PredicateNode::make(greater_than_equals_(a_a, 90),
            PredicateNode::make(less_than_(a_c, 500),
              node_a))))));


  const auto expected_optimized_lqp =
  PredicateNode::make(greater_than_(a_b, 40),
    PredicateNode::make(greater_than_(a_b, 50),
      PredicateNode::make(equals_(a_a, 42),
        ProjectionNode::make(expression_vector(a_a, a_b, a_c),
          PredicateNode::make(less_than_(a_c, 500),
            PredicateNode::make(greater_than_equals_(a_a, 90),
              node_a))))));
  // clang-format on

  const auto reordered_input_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_optimized_lqp);
}

TEST_F(PredicateReorderingRuleTest, SameOrderingForStoredTable) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_(b_a, 20),
    PredicateNode::make(less_than_(b_a, 40),
      node_b));

  const auto expected_lqp =
  PredicateNode::make(less_than_(b_a, 40),
    PredicateNode::make(less_than_(b_a, 20),
      node_b));
  // clang-format on

  const auto actual_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingRuleTest, PredicatesAsRightInput) {
  /**
   * Check that Reordering predicates works if a_a predicate chain is both on the left and right side of a_a node_a.
   * This is particularly interesting because the PredicateReorderingRule needs to re-attach the ordered chain of
   * predicates to the output (the cross node_a in this case). This test checks whether the attachment happens as the
   * correct input.
   *
   *             _______Cross________
   *            /                    \
   *  Predicate_0(a_a > 80)     Predicate_2(a_a > 90)
   *           |                     |
   *  Predicate_1(a_a > 60)     Predicate_3(a_a > 50)
   *           |                     |
   *        Table_0           Predicate_4(a_a > 30)
   *                                 |
   *                               Table_1
   */
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(greater_than_(a_a, 80),
      PredicateNode::make(greater_than_(a_a, 60),
        node_a)),
    PredicateNode::make(greater_than_(b_a, 90),
      PredicateNode::make(greater_than_(b_a, 50),
        PredicateNode::make(greater_than_(b_a, 30),
          node_b))));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(greater_than_(a_a, 60),
      PredicateNode::make(greater_than_(a_a, 80),
        node_a)),
    PredicateNode::make(greater_than_(b_a, 30),
      PredicateNode::make(greater_than_(b_a, 50),
        PredicateNode::make(greater_than_(b_a, 90),
          node_b))));
  // clang-format on

  const auto actual_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingRuleTest, PredicatesWithMultipleOutputs) {
  /**
   * If a_a PredicateNode has multiple outputs, it should not be considered for reordering
   */
  /**
   *      _____Union___
   *    /             /
   * Predicate_a     /
   *    \           /
   *     Predicate_b
   *         |
   *       Table
   *
   * predicate_a should come before predicate_b - but since Predicate_b has two outputs, it can't be reordered
   */

  const auto predicate_b = PredicateNode::make(greater_than_(a_b, 90), node_a);

  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, 90),
      predicate_b,
    predicate_b));

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, 90),
      predicate_b,
    predicate_b));
  // clang-format on

  const auto actual_lqp = RuleBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
