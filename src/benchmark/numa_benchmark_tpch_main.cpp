#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "numa_benchmark_runner.hpp"
#include "cxxopts.hpp"
#include "json.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

#include "cpucounters.h" // PCM lib

/**
 * This benchmark measures Hyrise's performance executing the TPC-H *queries*, it doesn't (yet) support running the
 * TPC-H *benchmark* exactly as it is specified.
 * (Among other things, the TPC-H requires performing data refreshes and has strict requirements for the number of
 * sessions running in parallel. See http://www.tpc.org/tpch/default.asp for more info)
 * The benchmark offers a wide range of options (scale_factor, chunk_size, ...) but most notably it offers two modes:
 * IndividualQueries and PermutedQuerySets. See docs on BenchmarkMode for details.
 * The benchmark will stop issuing new queries if either enough iterations have taken place or enough time has passed.
 *
 * main() is mostly concerned with parsing the CLI options while NumaBenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

int main(int argc, char* argv[]) {
  auto cli_options = opossum::NumaBenchmarkRunner::get_basic_cli_options("TPCH Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("0.001"))
    ("q,queries", "Specify queries to run, default is all that are supported", cxxopts::value<std::vector<opossum::QueryID>>()); // NOLINT
  // clang-format on

  std::unique_ptr<opossum::BenchmarkConfig> config;
  std::vector<opossum::QueryID> query_ids;
  float scale_factor;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    scale_factor = json_config.value("scale", 0.001f);
    query_ids = json_config.value("queries", std::vector<opossum::QueryID>());

    config = std::make_unique<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    // Display usage and quit
    if (cli_parse_result.count("help")) {
      std::cout << opossum::CLIConfigParser::detailed_help(cli_options) << std::endl;
      return 0;
    }

    if (cli_parse_result.count("queries")) {
      query_ids = cli_parse_result["queries"].as<std::vector<opossum::QueryID>>();
    }

    scale_factor = cli_parse_result["scale"].as<float>();

    config =
        std::make_unique<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // Build list of query ids to be benchmarked and display it
  if (query_ids.empty()) {
    std::transform(opossum::tpch_queries.begin(), opossum::tpch_queries.end(), std::back_inserter(query_ids),
                   [](auto& pair) { return pair.first; });
  }

  config->out << "- Benchmarking Queries: [ ";
  for (const auto query_id : query_ids) {
    config->out << (query_id) << ", ";
  }
  config->out << "]" << std::endl;

  // Initialize PCM lib
  config->out << std::endl << "Initializing PCM lib" << std::endl;
  PCM * pcm = PCM::getInstance();
  // pcm->resetPMU();
  // exit(0);
  PCM::ErrorCode returnResult = pcm->program();
  if (returnResult != PCM::Success){
    std::cerr << "PCM couldn't start" << std::endl;
    std::cerr << "Error code: " << returnResult << std::endl;
    exit(1);
  }
  auto number_of_sockets = pcm->getNumSockets();
  auto links_per_socket = pcm->getQPILinksPerSocket();
  config->out << std::endl;

  // Set up TPCH benchmark
  opossum::NamedQueries queries;
  queries.reserve(query_ids.size());

  for (const auto query_id : query_ids) {
    queries.emplace_back("TPC-H " + std::to_string(query_id), opossum::tpch_queries.at(query_id));
  }

  config->out << "- Generating TPCH Tables with scale_factor=" << scale_factor << " ..." << std::endl;

  const auto tables = opossum::TpchDbGenerator(scale_factor, config->chunk_size).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = opossum::tpch_table_names.at(tpch_table.first);
    auto& table = tpch_table.second;

    opossum::BenchmarkTableEncoder::encode(table_name, table, config->encoding_config);
    opossum::StorageManager::get().add_table(table_name, table);
  }
  config->out << "- ... done." << std::endl;

  auto context = opossum::NumaBenchmarkRunner::create_context(*config);

  // Add NUMA-specific information
  context.emplace("number_of_sockets", number_of_sockets);
  context.emplace("links_per_socket", links_per_socket);

  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);

  // Run the benchmark
  opossum::NumaBenchmarkRunner(*config, queries, context).run();

  // Deinitialize PCM lib (IMPORTANT!!)
  config->out << std::endl << "Cleaning up PCM lib" << std::endl;
  pcm->cleanup();
}
