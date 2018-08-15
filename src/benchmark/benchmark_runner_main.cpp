#include <cxxopts.hpp>

#include "numa_benchmark_runner.hpp"
#include "benchmark_utils.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "tpch/tpch_queries.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

#include "cpucounters.h" // PCM lib

int main(int argc, char* argv[]) {
  auto cli_options = opossum::NumaBenchmarkRunner::get_basic_cli_options("Hyrise Benchmark Runner");

  // clang-format off
  cli_options.add_options()
      ("tables", "Specify tables to load, either a single .csv/.tbl file or a directory with these files", cxxopts::value<std::string>()->default_value("")) // NOLINT
      ("queries", "Specify queries to run, either a single .sql file or a directory with these files", cxxopts::value<std::string>()->default_value("")) // NOLINT
      ("pcm", "Add PCM measurements (sudo needed)", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  std::unique_ptr<opossum::BenchmarkConfig> config;
  std::string query_path;
  std::string table_path;
  bool pcm;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    table_path = json_config.value("tables", "");
    query_path = json_config.value("queries", "");
    pcm = json_config.value("pcm", false);

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

    pcm = cli_parse_result["pcm"].as<bool>();

    query_path = cli_parse_result["queries"].as<std::string>();
    table_path = cli_parse_result["tables"].as<std::string>();

    config =
        std::make_unique<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // Check that the options 'queries' and 'tables' were specifiedc
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --queries=path/to/queries and --tables=path/to/tables" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  config->out << "- Benchmarking queries from " << query_path << std::endl;
  config->out << "- Running on tables from " << table_path << std::endl;

  auto number_of_sockets = uint32_t{1};
  auto links_per_socket = uint64_t{1};

  PCM * pcmInstance = nullptr;

  if (pcm) {
    // Initialize PCM lib
    config->out << std::endl << "Initializing PCM lib" << std::endl;
    pcmInstance = PCM::getInstance();
    // pcmInstance->resetPMU();
    // exit(0);
    PCM::ErrorCode returnResult = pcmInstance->program();
    if (returnResult != PCM::Success){
      std::cerr << "PCM couldn't start" << std::endl;
      std::cerr << "Error code: " << returnResult << std::endl;
      exit(1);
    }
    number_of_sockets = pcmInstance->getNumSockets();
    links_per_socket = pcmInstance->getQPILinksPerSocket();
    config->out << std::endl;
  }

  opossum::NumaBenchmarkRunner::create_tables(*config, table_path);
  const auto queries = opossum::NumaBenchmarkRunner::_read_query_folder(query_path);
  auto context = opossum::NumaBenchmarkRunner::create_context(*config);

  if (pcm) {
    // Add NUMA-specific information
    context.emplace("number_of_sockets", number_of_sockets);
    context.emplace("links_per_socket", links_per_socket);
  }

  // Run the benchmark
  opossum::NumaBenchmarkRunner(*config, queries, context).run();
  // opossum::NumaBenchmarkRunner::create(*config, table_path, query_path).run();

  if (pcm) {
    // Deinitialize PCM lib (IMPORTANT!!)
    config->out << std::endl << "Cleaning up PCM lib" << std::endl;
    pcmInstance->cleanup();
  }
}
