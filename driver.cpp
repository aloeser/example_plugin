#include <fstream>
#include <unordered_set>

#include <boost/algorithm/string.hpp>

#include "driver.hpp"
#include "plan_cache_csv_exporter.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "boost/variant/get.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#define MACOS OS
#include "tpcds/tpcds_table_generator.hpp"

using namespace opossum;  // NOLINT

namespace {

// Shamelessly copied from tpcds_benchmark.cpp
const std::unordered_set<std::string> filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "hyrise/resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }
  return filename_blacklist;
}

void extract_table_meta_data(const std::string folder_name) {
  // TODO: why not use the CSV exporter?
  auto table_to_csv = [](const std::string table_name, const std::string csv_file_name, const bool show_distinct_value_count = false) {
    const auto table = SQLPipelineBuilder{"SELECT * FROM " + table_name}
                          .create_pipeline()
                          .get_result_table().second;
    std::ofstream output_file(csv_file_name);

    const auto column_names = table->column_names();
    for (auto column_id = size_t{0}; column_id < column_names.size(); ++column_id) {
      auto column_name = column_names[column_id];
      boost::to_upper(column_name);
      output_file << column_name;
      if (column_id < (column_names.size() - 1)) {
        output_file << "|";
      }
    }

    if (show_distinct_value_count) {
      output_file << "|DISTINCT_VALUES|IS_GLOBALLY_SORTED";
    }
    output_file << std::endl;

    const auto row_count = table->row_count();
    const auto data_types = table->column_data_types();
    for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
      const auto row = table->get_row(row_index);
      for (auto column_id = size_t{0}; column_id < row.size(); ++column_id) {
        if (data_types[column_id] == DataType::String) output_file << "\"";
        output_file << row[column_id];
        if (data_types[column_id] == DataType::String) output_file << "\"";
        if (column_id < (row.size() - 1)) {
          output_file << "|";
        }
      }
      if (show_distinct_value_count) {
        const auto benchmark_table_name = boost::lexical_cast<std::string>(row[0]);
        const auto benchmark_table = Hyrise::get().storage_manager.get_table(benchmark_table_name);
        Assert(benchmark_table, "could not get table " + benchmark_table_name);

        const auto table_statistics = benchmark_table->table_statistics();
        const auto benchmark_column_name = boost::lexical_cast<std::string>(row[1]);
        const auto benchmark_column_id = benchmark_table->column_id_by_name(benchmark_column_name);
        const auto benchmark_column_type = benchmark_table->column_data_type(benchmark_column_id);

        auto distinct_value_count = -1;

        resolve_data_type(benchmark_column_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          const auto table_attribute_statistics = table_statistics->column_statistics[benchmark_column_id];
          const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(table_attribute_statistics);
          Assert(attribute_statistics, "could not cast to AttributeStatistics");
          const auto histogram = attribute_statistics->histogram;
          if (!histogram) {
            std::cout << "no histogram available for column " << benchmark_column_name << " of table " << benchmark_table_name << std::endl;
            distinct_value_count = 1;
          } else {
            distinct_value_count = histogram->total_distinct_count();
          }
        });
        output_file << "|" << distinct_value_count;


        resolve_data_type(benchmark_column_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          std::optional<ColumnDataType> old_max;
          bool assume_sorted = true;

          for (ChunkID chunk_id{0}; chunk_id < benchmark_table->chunk_count(); chunk_id++) {
            const auto& chunk = benchmark_table->get_chunk(chunk_id);
            if (chunk) {
              Assert(chunk->pruning_statistics(), "Chunk has no pruning statistics");
              const auto& pruning_statistics = *chunk->pruning_statistics();
              Assert(benchmark_column_id < pruning_statistics.size(), "benchmark_column_id out of bounds");
              const auto& column_attribute_statistics = pruning_statistics[benchmark_column_id];
              const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(column_attribute_statistics);
              Assert(attribute_statistics, "could not cast to AttributeStatistics");


              std::cout << "Comparing" << std::endl;
              if constexpr (std::is_arithmetic_v<ColumnDataType>) {
                const auto range_filter = attribute_statistics->range_filter;

                // TODO if there is no range filter, assume there are only NULLS. See comment below for reasoning
                if (!range_filter){
                  assume_sorted = false;
                  break;
                }

                Assert(range_filter, "no range filter despite arithmetic type");
                const auto& ranges = range_filter->ranges;
                if (old_max && *old_max > ranges[0].first) {
                  assume_sorted = false;
                  break;
                }
                old_max = ranges.back().second;
              } else {
                const auto min_max_filter = attribute_statistics->min_max_filter;

                // TODO find out why some columns do not have a min max filter
                // TODO I assume its because all values in the segment are NULL
                // TODO Since the data is not supposed to be sorted, it is unlikely (but possible!)
                // TODO that a whole segment is NULL. Thus assume there is only NULL, and early exit.
                if (!min_max_filter) {
                  assume_sorted = false;
                  break;
                }

                Assert(min_max_filter, "no min max filter despite non-arithmetic type");
                if (old_max && *old_max > min_max_filter->min) {
                  assume_sorted = false;
                  break;
                }
                old_max = min_max_filter->max;
              }
            }
          }

          if (benchmark_table->chunk_count() <= 1 || distinct_value_count <= 1) {
            assume_sorted = false;
          }

          output_file << "|" << assume_sorted;
        });
      }
      output_file << std::endl;
    }
  };

  //table_to_csv("meta_segments", folder_name + "/segment_meta_data2.csv");
  table_to_csv("meta_segments_accurate", folder_name + "/segment_meta_data.csv");
  table_to_csv("meta_tables", folder_name + "/table_meta_data.csv");
  table_to_csv("meta_columns", folder_name + "/column_meta_data.csv", true);
}

}  // namespace

std::string Driver::description() const { return "This driver executes benchmarks and outputs its plan cache to an array of CSV files."; }

void Driver::start() {
  const auto BENCHMARKS = std::vector<std::string>{"TPC-H", "TPC-DS", "JOB"};

  const auto env_var = std::getenv("BENCHMARK_TO_RUN");
  if (env_var == NULL) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting Plugin." << std::flush;
    exit(17);
  }

  auto BENCHMARK = std::string(env_var);
  if (std::find(BENCHMARKS.begin(), BENCHMARKS.end(), BENCHMARK) == BENCHMARKS.end()) {
    std::cerr << "Benchmark \"" << BENCHMARK << "\" not supported. Supported benchmarks: ";
    for (const auto& benchmark : BENCHMARKS) std::cout << "\"" << benchmark << "\" ";
    std::cerr << "\nExiting." << std::flush;
    exit(17);
  }
  std::cout << "Running " << BENCHMARK << " ... " << std::endl;

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_runs = 10;
  config->enable_visualization = false;
  config->cache_binary_tables = false;
  config->max_duration = std::chrono::seconds(60);
  //config->warmup_duration = std::chrono::seconds(20);

  constexpr auto USE_PREPARED_STATEMENTS = false;
  auto SCALE_FACTOR = 17.0f;  // later overwritten
  const auto MAX_RUNTIME = 60;


  // Set caches
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>(100'000);
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>(100'000);


  //
  //  TPC-H
  //
  if (BENCHMARK == "TPC-H") {
    SCALE_FACTOR = 1.0f;
    config->max_runs = 10;
    config->warmup_duration = std::chrono::seconds(0);
    config->max_duration = std::chrono::seconds(MAX_RUNTIME);
    // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
    // auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
    auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(
        *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, config), BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;

    const std::filesystem::path plugin_path("/home/Alexander.Loeser/example_plugin/build-release/lib/libhyriseClusteringPlugin.so");
    Hyrise::get().plugin_manager.load_plugin(plugin_path);

    benchmark_runner->run();
  }
  //
  //  /TPC-H
  //


  //
  //  TPC-DS
  //
  else if (BENCHMARK == "TPC-DS") {
    SCALE_FACTOR = 1.0f;
    config->max_runs = 1;
    config->warmup_duration = std::chrono::seconds(0);
    config->max_duration = std::chrono::seconds(MAX_RUNTIME);
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";
    if (!std::filesystem::exists("resources/")) {
      std::cout << "When resources for TPC-DS cannot be found on Linux, create a symlink as a workaround: 'ln -s hyrise/resources resources'." << std::endl;
    }

    auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
    auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, config);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                                              opossum::BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;

    const std::filesystem::path plugin_path("/home/Alexander.Loeser/example_plugin/build-release/lib/libhyriseClusteringPlugin.so");
    Hyrise::get().plugin_manager.load_plugin(plugin_path);

    benchmark_runner->run();
  }
  //
  //  /TPC-DS
  //

  //
  //  JOB
  //
  else if (BENCHMARK == "JOB") {
    config->max_runs = 1;

    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                                              BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /JOB
  //

  const std::string folder_name = std::string(BENCHMARK) + "__SF_" + std::to_string(SCALE_FACTOR) + "__RUNS_" + std::to_string(config->max_runs) + "__TIME_" + std::to_string(config->max_duration.count() / 1000000000);
  std::filesystem::create_directories(folder_name);

  std::cout << "Exporting table/column/segments meta data." << std::endl;
  extract_table_meta_data(folder_name);

  if (Hyrise::get().default_pqp_cache->size() > 0) {
    std::cout << "Exporting plan cache data." << std::endl;
    PlanCacheCsvExporter(folder_name).run();
  } else {
    std::cerr << "Plan cache is empty." << std::endl;
    exit(17);
  }

  std::cout << "Done." << std::endl;
}

void Driver::stop() {
}

EXPORT_PLUGIN(Driver)
