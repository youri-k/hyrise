#include <iostream>

#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "benchmark_runner.hpp"
#include "benchmark_config.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto scale_factor = 0.001f;
  auto config = BenchmarkConfig::get_default_config();

  BenchmarkRunner(config, std::make_unique<TPCHQueryGenerator>(true, scale_factor),
      std::make_unique<TpchTableGenerator>(scale_factor, std::make_shared<BenchmarkConfig>(config)), 100'000)
      .run();

  // create meta data, data ... dataa
  for (auto& table_name : StorageManager::get().table_names()) {
    const auto& table = StorageManager::get().get_table(table_name);

    std::cout << table_name << "," << table->row_count() << "," << table->max_chunk_size() << std::endl;

    for (const auto& column_def : table->column_definitions()) {
      std::cout << table_name << "," << column_def.name << ","
                << data_type_to_string.left.at(column_def.data_type) << "," << column_def.nullable
                << "\n";

      for (auto chunk_id = ChunkID{0}, end = table->chunk_count(); chunk_id < end; ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto column_id = table->column_id_by_name(column_def.name);
        const auto& segment = chunk->get_segment(column_id);

        const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
        const auto encoding_type = encoded_segment->encoding_type();

        std::cout << table_name << "," << column_def.name << "," << chunk_id << ","
                  << encoding_type_to_string.left.at(encoding_type) << ",";

        std::string vector_compression_str = "Invalid";
        if (encoded_segment->compressed_vector_type()) {
	        switch (*encoded_segment->compressed_vector_type()) {
	          case CompressedVectorType::FixedSize4ByteAligned:
	            vector_compression_str = "FSBA32bit";
	            break;
	          case CompressedVectorType::FixedSize2ByteAligned:
	            vector_compression_str = "FSBA16bit";
	            break;
	          case CompressedVectorType::FixedSize1ByteAligned:
	            vector_compression_str = "FSBA8bit";
	            break;
	          case CompressedVectorType::SimdBp128:
	            vector_compression_str = "SIMDBP128";
	            break;
	          default:
	            vector_compression_str = "Invalid";
	        }
        }
        // std::cout << vector_compression_type_to_string.left.at(static_cast<VectorCompressionType>(compressed_vector_type)) << std::endl;
        std::cout << vector_compression_str << "\n";
      }
    }
  }


  // get "workload"
  // start of (query count >> )

  for (const auto& [query_str, query_plan] : SQLPhysicalPlanCache::get()) {
    std::cout << "=== Q (" << query_str.substr(0, 60) << " ...)" << std::endl;

    query_plan->print();

    visit_lqp(cache_result, [&](const auto& node) {
      if (node->type == LQPNodeType::Predicate) {
        std::cout << "Predicate node: " << node->description() << std::endl;
        auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

        const auto operator_predicates =
            OperatorScanPredicate::from_expression(*predicate_node->predicate, *node);
        Assert(operator_predicates.value().size() < 2, "Unexpected predicate with more than one predicate");
        std::cout << operator_predicates.value().size() << std::endl;
        std::cout << operator_predicates.value()[0].to_string() << std::endl;

        auto original_column_id = ColumnID{23'000};
        std::string table_name = "";

        for (const auto el : predicate_node->node_expressions()) {
          visit_expression(el, [&](const auto& expression) {
            if (expression->type == ExpressionType::LQPColumn) {
              const auto ce = std::dynamic_pointer_cast<LQPColumnExpression>(expression);

              const auto cr = ce->column_reference;
              const auto original_node = cr.original_node();

              if (original_node->type == LQPNodeType::StoredTable) {
                const auto stored_table_node =
                    std::dynamic_pointer_cast<const StoredTableNode>(original_node);
                table_name = stored_table_node->table_name;
                const auto& table = StorageManager::get().get_table(table_name);
                original_column_id = cr.original_column_id();
                std::cout << "Table: " << table_name << " - col: " << table->column_name(original_column_id)
                          << std::endl;
                std::cout << predicate_node->derive_statistics_from(predicate_node->left_input())->row_count()
                          << std::endl;
                std::cout << "Selectivity: "
                          << predicate_node->derive_statistics_from(predicate_node->left_input())->row_count() /
                                 table->row_count()
                          << std::endl;
              }
            }
            return ExpressionVisitation::VisitArguments;
          });
        }

        if (predicate_node->predicate->type == ExpressionType::Predicate) {
          auto predicate_expression =
              std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate);

          auto table_statistics = StorageManager::get().get_table(table_name)->table_statistics();
          std::cout << "$$$$$$$$$$"
                    << table_statistics
                           ->estimate_predicate(operator_predicates.value()[0].column_id,
                                                operator_predicates.value()[0].predicate_condition,
                                                operator_predicates.value()[0].value,
                                                operator_predicates.value()[0].value2)
                           .row_count()
                    << std::endl;

          // std::cout << "$$$$$$$$$$" << table_statistics->estimate_predicate(original_column_id, predicate_expression->predicate_condition, NullValue{}, std::nullopt).row_count() << std::endl;
        }
      }
      return LQPVisitation::VisitInputs;
    });
  }

  // std::cout << "%%%%%%%%%%%" << SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get().size() << std::endl;
  // std::cout << "%%%%%%%%%%%" << SQLQueryCache<SQLQueryPlan>::get().size() << std::endl;

  // auto& cache = SQLQueryCache<SQLQueryPlan>::get().cache();
  // auto gdfs_cache = dynamic_cast<GDFSCache<std::string, SQLQueryPlan>&>(cache);

  // for (const auto& [key, value] : gdfs_cache) {
  //   std::cout << key << " : ";
  //   // value.print();
  //   for (auto& op : value.tree_roots()) {
  //     op->print();
  //   }
  //   std::cout << std::endl;
  // }
}
