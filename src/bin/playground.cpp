#include <fstream>
#include <sstream>
#include <string>
#include <iomanip>
#include <iostream>
#include <filesystem>
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
#include "hyrise.hpp"
#include "types.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "boost/functional/hash.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
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

  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    const auto& table = Hyrise::get().storage_manager.get_table(table_name);

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
  constexpr auto SCALE_FACTOR = 1.0f;

  auto start_config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  start_config->max_runs = 5;
  start_config->enable_visualization = false;
  start_config->chunk_size = 100'000;
  start_config->cache_binary_tables = true;

  // const std::vector<BenchmarkItemID> tpch_query_ids_warmup = {BenchmarkItemID{5}};
  const bool use_prepared_statements = false;
  auto start_context = BenchmarkRunner::create_context(*start_config);

  auto start_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(start_config, use_prepared_statements, SCALE_FACTOR);//, tpch_query_ids_warmup);
  BenchmarkRunner(*start_config, std::move(start_item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, start_config), start_context).run();

  std::string path = "/Users/martin/Programming/compression_selection_python/configurations_debug";
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    const auto conf_path = entry.path();
    const auto conf_name = conf_path.stem();
    const auto filename = conf_path.filename().string();

    if (filename.find("conf") != 0 || filename.find(".json") != std::string::npos) {
      std::cout << "Skipping " << conf_path << std::endl;
      continue;
    }

    std::cout << "Benchmarking " << conf_name << " ..." << std::endl;

    {
      auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
      config->max_runs = 10;
      config->enable_visualization = false;
      config->output_file_path = conf_name.string() + ".json";
      config->chunk_size = 100'000;
      config->cache_binary_tables = true;

      auto context = BenchmarkRunner::create_context(*config);

      std::ifstream configuration_file(entry.path().string());
      std::string line;
      std::cout << "Reencoding: " << std::flush;
      while (std::getline(configuration_file, line))
      {
        std::vector<std::string> line_values;
        std::istringstream linestream(line);
        std::string value;
        while (std::getline(linestream, value, ','))
        {
          line_values.push_back(value);
        }

        const auto table_name = line_values[0];
        const auto column_name = line_values[1];
        const auto chunk_id = std::stoi(line_values[2]);
        const auto encoding_type_str = line_values[3];
        const auto vector_compression_type_str = line_values[4];

        const auto& table = Hyrise::get().storage_manager.get_table(table_name);
        const auto& chunk = table->get_chunk(ChunkID{static_cast<uint32_t>(chunk_id)});
        const auto& column_id = table->column_id_by_name(column_name);
        const auto& segment = chunk->get_segment(column_id);
        const auto& data_type = table->column_data_type(column_id);

        const auto encoding_type = encoding_type_to_string.right.at(encoding_type_str);

        SegmentEncodingSpec spec = {encoding_type};
        if (vector_compression_type_str != "None") {
          const auto vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_type_str);
          spec.vector_compression_type = vector_compression_type;
        }

        const auto& encoded_segment = ChunkEncoder::encode_segment(segment, data_type, spec);
        chunk->replace_segment(column_id, encoded_segment);
        std::cout << "." << std::flush;
      }
      std::cout << " done." << std::endl;
      configuration_file.close();
      // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{1}};

      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, SCALE_FACTOR);//, tpch_query_ids_benchmark);
      BenchmarkRunner(*config, std::move(item_runner), nullptr, context).run();

      // TableIdentifierMap table_name_table_id_to_map;
      // AttributeIdentifierMap attribute_to_attribute_id_map;

      // extract_meta_data(table_name_table_id_to_map, attribute_to_attribute_id_map);
    }
  }
  return 0;
}
