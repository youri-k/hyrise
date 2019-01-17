#pragma once

#include "expression/abstract_expression.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace opossum {

class AbstractLQPNode;

class LQPVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractLQPNode>>> {
 public:
  LQPVisualizer();

  LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                VizEdgeInfo edge_info = {});

 protected:
  void _build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots) override;

  void _build_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                      std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visualized_nodes,
                      ExpressionUnorderedSet& visualized_sub_queries);

  void _build_dataflow(const std::shared_ptr<AbstractLQPNode>& from, const std::shared_ptr<AbstractLQPNode>& to);
};

}  // namespace opossum
