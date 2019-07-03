#include <fstream>
#include <iomanip>
#include <iostream>
#include <boost/bimap.hpp>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_plan_cache.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "boost/functional/hash.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/index/base_index.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"

using namespace opossum;  // NOLINT

constexpr auto SCALE_FACTOR = 1.0f;

typedef boost::bimap<std::string, uint16_t> table_name_id_bimap;
typedef table_name_id_bimap::value_type table_name_id;

table_name_id_bimap table_name_id_map;

using TableColumnIdentifier = std::pair<uint16_t, ColumnID>;

using TableIdentifierMap = std::map<std::string, uint16_t>;

// maps an attribute to an identifier in the form of `table_idenfier_column_id`
using AttributeIdentifierMap = std::map<std::pair<std::string, std::string>, std::string>;

namespace std {
template <>
struct hash<TableColumnIdentifier> {
  size_t operator()(TableColumnIdentifier const& v) const {
    const std::string combined = std::to_string(v.first) + "_" + std::to_string(v.second);
    return std::hash<std::string>{}(combined);
  }
};
}  // namespace std

void extract_meta_data(TableIdentifierMap& table_name_table_id_to_map,
                       AttributeIdentifierMap& attribute_to_attribute_id_map) {
  std::ofstream table_meta_data_csv_file("table_meta_data.csv");
  table_meta_data_csv_file << "TABLE_ID,TABLE_NAME,ROW_COUNT,MAX_CHUNK_SIZE\n";

  std::ofstream attribute_meta_data_csv_file("attribute_meta_data.csv");
  attribute_meta_data_csv_file << "ATTRIBUTE_ID,TABLE_NAME,COLUMN_NAME,DATA_TYPE,DISTINCT_VALUE_COUNT,IS_NULLABLE\n";

  std::ofstream segment_meta_data_csv_file("segment_meta_data.csv");
  segment_meta_data_csv_file << "ATTRIBUTE_ID,TABLE_NAME,COLUMN_NAME,CHUNK_ID,ENCODING,COMPRESSION,ROW_COUNT,SIZE_IN_BYTES\n";

  uint16_t next_table_id = 0;
  uint16_t next_attribute_id = 0;

  for (const auto& table_name : StorageManager::get().table_names()) {
    const auto& table = StorageManager::get().get_table(table_name);

    // might be an overkill here?
    const auto table_id = next_table_id;
    const auto& [table_iter, table_inserted] = table_name_table_id_to_map.try_emplace(table_name, table_id);
    if (table_inserted) {
      ++next_table_id;
    }

    table_meta_data_csv_file << table_id << "," << table_name << "," << table->row_count() << ","
                             << table->max_chunk_size() << std::endl;

    next_attribute_id = 0;
    for (const auto& column_def : table->column_definitions()) {
      const auto& column_name = column_def.name;
      const auto& data_type = column_def.data_type;
      const auto column_id = table->column_id_by_name(column_name);

      std::stringstream attr_table_id;
      attr_table_id << table_id << "_" << next_attribute_id;
      attribute_to_attribute_id_map.emplace(std::make_pair(table_name, column_name), attr_table_id.str());
      ++next_attribute_id;

      auto distinct_value_count = size_t{0};
      if (table->table_statistics() && column_id < table->table_statistics()->column_statistics.size()) {
        const auto base_attribute_statistics = table->table_statistics()->column_statistics[column_id];
	    resolve_data_type(data_type, [&](const auto column_data_type) {
	      using ColumnDataType = typename decltype(column_data_type)::type;

	      const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(base_attribute_statistics);
	      if (attribute_statistics) {
	        const auto equal_distinct_count_histogram = std::dynamic_pointer_cast<EqualDistinctCountHistogram<ColumnDataType>>(attribute_statistics->histogram);
	        if (equal_distinct_count_histogram) {
	          distinct_value_count = static_cast<size_t>(equal_distinct_count_histogram->total_distinct_count());
	        }
	      }
	    });
	  }

      // TODO(Bouncner): get distinct count via histogram as soon as we have merged the current master
      attribute_meta_data_csv_file << attr_table_id.str() << "," << table_name << "," << column_name << ","
                                   << data_type_to_string.left.at(column_def.data_type) << "," << distinct_value_count << ","
                                   << (column_def.nullable ? "TRUE" : "FALSE") << "\n";

      for (auto chunk_id = ChunkID{0}, end = table->chunk_count(); chunk_id < end; ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

        const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
        const auto encoding_type = encoded_segment->encoding_type();

        segment_meta_data_csv_file << attr_table_id.str() << "," << table_name << "," << column_name << "," << chunk_id << "," << encoding_type_to_string.left.at(encoding_type) << ",";

        if (encoded_segment->compressed_vector_type()) {
          switch (*encoded_segment->compressed_vector_type()) {
            case CompressedVectorType::FixedSize4ByteAligned: {
              segment_meta_data_csv_file << "FixedSize4ByteAligned";
              break;
            }
            case CompressedVectorType::FixedSize2ByteAligned: {
              segment_meta_data_csv_file << "FixedSize2ByteAligned";
              break;
            }
            case CompressedVectorType::FixedSize1ByteAligned: {
              segment_meta_data_csv_file << "FixedSize1ByteAligned";
              break;
            }
            case CompressedVectorType::SimdBp128: {
              segment_meta_data_csv_file << "SimdBp128";
              break;
            }
            default:
              segment_meta_data_csv_file << "NONE";
          }
        }

        segment_meta_data_csv_file << "," << segment->size() << "," << encoded_segment->estimate_memory_usage() << "\n";
      }
    }
  }

  table_meta_data_csv_file.close();
  attribute_meta_data_csv_file.close();
  segment_meta_data_csv_file.close();
}

int main(int argc, const char* argv[]) {
  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_runs = 1;
  config->enable_visualization = false;
  config->output_file_path = "perf.json";
  config->chunk_size = 100'000;
  config->cache_binary_tables = true;

  const std::vector<BenchmarkItemID> tpch_query_ids = {BenchmarkItemID{6}};
  const bool use_prepared_statements = false;

  auto context = BenchmarkRunner::create_context(*config);

  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, SCALE_FACTOR, tpch_query_ids);
  BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TpchTableGenerator>(SCALE_FACTOR, config), context)
      .run();


  // StorageManager::get().add_benchmark_runner(br);
  // auto& query_gen = br->query_generator();
  // br->run();

  TableIdentifierMap table_name_table_id_to_map;
  AttributeIdentifierMap attribute_to_attribute_id_map;

  extract_meta_data(table_name_table_id_to_map, attribute_to_attribute_id_map);

  return 0;
}
