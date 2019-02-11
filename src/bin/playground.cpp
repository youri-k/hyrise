#include <iostream>
#include <fstream>
#include <iomanip>

#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "benchmark_runner.hpp"
#include "benchmark_config.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace opossum;  // NOLINT

using TableIdentifierMap = std::map<std::string, uint16_t>;

// maps an attribute to an identifier in the form of `table_idenfier_column_id`
using AttributeIdentifierMap = std::map<std::pair<std::string, std::string>, std::string>;

void process_pqp(const std::shared_ptr<opossum::AbstractOperator> op) {
  // TODO: handle diamonds

  return;  
}

void extract_plan_cache_data(const TableIdentifierMap& table_name_table_id_to_map,
        const AttributeIdentifierMap& attribute_to_attribute_id_map, const std::string query_string,
        const std::shared_ptr<opossum::AbstractLQPNode> lqp_node) {
  if (lqp_node->type == LQPNodeType::CreatePreparedPlan) { return; }

  lqp_node->print();

  std::cout << "=== Q (" << query_string.substr(0, 60) << " ...)" << std::endl;

  std::ofstream plan_cache_csv_file("plan_cache.csv");
  plan_cache_csv_file << "QUERY_HASH,EXECUTION_COUNT,QUERY_STRING\n";

  std::ofstream sequential_accesses_csv_file("sequential_accesses.csv");
  sequential_accesses_csv_file << "QUERY_HASH,COLUMN_ID,OPERATION,PREDICATE,SELECTIVITY_IN_LQP,SELECTIITY_INDEPENDENT,ORDER_IN_LQP,IS_REFERENCE_ACCESS\n";

  std::stringstream query_hex_hash;
  query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

  auto query_string2 = query_string;
  query_string2.erase(std::remove(query_string2.begin(), query_string2.end(), '\n'), query_string2.end());

  plan_cache_csv_file << query_hex_hash.str() << ",1," << query_string2 << "\n";

  visit_lqp(lqp_node, [&](const auto& node) {
    if (node->type == LQPNodeType::Predicate) {
      std::cout << "Predicate node: " << node->description() << std::endl;
      auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

      const auto operator_predicates =
          OperatorScanPredicate::from_expression(*predicate_node->predicate(), *node);
      Assert(operator_predicates->size() < 2, "Unexpected predicate with more than one predicate");
      std::cout << operator_predicates.value().size() << std::endl;
      std::cout << operator_predicates.value()[0].to_string() << std::endl;

      auto original_column_id = ColumnID{23'000};
      std::string table_name = "$&$&404";
      std::string column_name = "$&$&404";
      long row_count_current_table = -1;
      double selectivity = -1.0f;
      double selectivity_independent = -1.0f;

      for (const auto& el : lqp_node->node_expressions) {
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
              column_name = table->column_name(original_column_id);
              std::cout << "Table: " << table_name << " - col: " << table->column_name(original_column_id)
                        << std::endl;
              std::cout << predicate_node->derive_statistics_from(predicate_node->left_input())->row_count()
                        << std::endl;
              std::cout << "Selectivity: "
                        << predicate_node->derive_statistics_from(predicate_node->left_input())->row_count() /
                               table->row_count()
                        << std::endl;
              selectivity = predicate_node->derive_statistics_from(predicate_node->left_input())->row_count() /
                               table->row_count();

              row_count_current_table = table->row_count();
            }
          }
          return ExpressionVisitation::VisitArguments;
        });
      }

      if (predicate_node->predicate()->type == ExpressionType::Predicate) {
        auto predicate_expression =
            std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());

        auto table_statistics = StorageManager::get().get_table(table_name)->table_statistics();
        std::cout << "$$$$$$$$$$"
                  << table_statistics->estimate_predicate(operator_predicates.value()[0].column_id,
                                              operator_predicates.value()[0].predicate_condition,
                                              operator_predicates.value()[0].value,
                                              operator_predicates.value()[0].value2).row_count()
                  << std::endl;
        selectivity_independent = table_statistics->estimate_predicate(operator_predicates.value()[0].column_id,
                                              operator_predicates.value()[0].predicate_condition,
                                              operator_predicates.value()[0].value,
                                              operator_predicates.value()[0].value2).row_count() / row_count_current_table;
        // std::cout << "$$$$$$$$$$" << table_statistics->estimate_predicate(original_column_id, predicate_expression->predicate_condition, NullValue{}, std::nullopt).row_count() << std::endl;
      }

      if (table_name != "$&$&404" && column_name != "$&$&404" && selectivity >= 0 && selectivity_independent >= 0 && row_count_current_table >= 0) {
        const auto const_pair = std::make_pair(table_name, column_name);
        // auto test = attribute_to_attribute_id_map[const_pair];
        sequential_accesses_csv_file << query_hex_hash.str() << "," << attribute_to_attribute_id_map.at(const_pair) << ",";
        switch (predicate_node->scan_type) {
          case ScanType::TableScan:
            sequential_accesses_csv_file << "table_scan,";
            break;;
          case ScanType::IndexScan:
            sequential_accesses_csv_file << "index_scan,";
        }
        sequential_accesses_csv_file << predicate_condition_to_string.left.at(operator_predicates.value()[0].predicate_condition) << ",";
        sequential_accesses_csv_file << selectivity << "," << selectivity_independent << ",";
        sequential_accesses_csv_file << "6666,";
        if (lqp_node->left_input()->type == LQPNodeType::StoredTable) {
          sequential_accesses_csv_file << "true\n";
        } else {
          sequential_accesses_csv_file << "false\n";
        }
      } else {
        std::cout << "An error occured." << std::endl;
      }
    }
    return LQPVisitation::VisitInputs;
  });

  plan_cache_csv_file.close();
  sequential_accesses_csv_file.close();
}

void extract_meta_data(TableIdentifierMap& table_name_table_id_to_map, AttributeIdentifierMap& attribute_to_attribute_id_map) {
  std::ofstream table_meta_data_csv_file("table_meta_data.csv");
  table_meta_data_csv_file << "TABLE_NAME,ROW_COUNT,MAX_CHUNK_SIZE\n";

  std::ofstream attribute_meta_data_csv_file("attribute_meta_data.csv");
  attribute_meta_data_csv_file << "ATTRIBUTE_ID,TABLE_NAME,COLUMN_NAME,DATA_TYPE,IS_NULLABLE\n";

  std::ofstream segment_meta_data_csv_file("segment_meta_data.csv");
  segment_meta_data_csv_file << "ATTRIBUTE_ID,TABLE_NAME,COLUMN_NAME,CHUNK_ID,ENCODING,COMPRESSION\n";

  uint16_t next_table_id = 0;
  uint16_t next_attribute_id = 0;

  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto& table = StorageManager::get().get_table(table_name);

    // might be an overkill here?
    const auto table_id = next_table_id;
    const auto& [table_iter, table_inserted] = table_name_table_id_to_map.try_emplace(table_name, table_id);
    if (table_inserted) { ++next_table_id; }

    table_meta_data_csv_file << table_id << "," << table_name << "," << table->row_count() << "," << table->max_chunk_size() << std::endl;

    next_attribute_id = 0;
    for (const auto& column_def : table->column_definitions()) {
      const auto& column_name = column_def.name;

      std::stringstream attr_table_id;
      attr_table_id << table_id << "_" << next_attribute_id;
      attribute_to_attribute_id_map.emplace(std::make_pair(table_name, column_name), attr_table_id.str());
      ++next_attribute_id;

      attribute_meta_data_csv_file << attr_table_id.str() << "," << table_name << "," << column_name << ","
                << data_type_to_string.left.at(column_def.data_type) << "," <<
                (column_def.nullable ? "TRUE" : "FALSE") << "\n";

      for (auto chunk_id = ChunkID{0}, end = table->chunk_count(); chunk_id < end; ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto column_id = table->column_id_by_name(column_name);
        const auto& segment = chunk->get_segment(column_id);

        const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
        const auto encoding_type = encoded_segment->encoding_type();

        segment_meta_data_csv_file << attr_table_id.str() << "," << table_name << "," << column_name << "," << chunk_id << ","
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
        segment_meta_data_csv_file << vector_compression_str << "\n";
      }
    }
  }

  table_meta_data_csv_file.close();
  attribute_meta_data_csv_file.close();
  segment_meta_data_csv_file.close();
}

int main() {
  const auto scale_factor = 0.01f;
  auto config = BenchmarkConfig::get_default_config();
  config.max_num_query_runs = 1;
  const std::vector<QueryID> tpch_query_ids = {QueryID{5}};

  BenchmarkRunner(config, std::make_unique<TPCHQueryGenerator>(true, scale_factor, tpch_query_ids),
      std::make_unique<TpchTableGenerator>(scale_factor, std::make_shared<BenchmarkConfig>(config)), 100'000)
      .run();

  TableIdentifierMap table_name_table_id_to_map;
  AttributeIdentifierMap attribute_to_attribute_id_map;

  extract_meta_data(table_name_table_id_to_map, attribute_to_attribute_id_map);

  // get "workload"
  for (const auto& [query_string, logical_query_plan] : SQLLogicalPlanCache::get()) {
    // TODO(Bouncner): check for GDFS, if so, pass flag ... use flag to retrieve access counts per element.
    // logical_query_plan->print();
    extract_plan_cache_data(table_name_table_id_to_map, attribute_to_attribute_id_map, query_string, logical_query_plan);
  }

  for (const auto& [query_string, physical_query_plan] : SQLPhysicalPlanCache::get()) {
    // std::cout << "=== Q (" << query_string.substr(0, 60) << " ...)\n\tskipping" << std::endl;
    // physical_query_plan->print();
    process_pqp(physical_query_plan);
  }

  return 0; 
}

//
// QUERY_LIST mit Execution count usw. >> Plan Cache
// sequential_accesses >> column_id,selectivity,
