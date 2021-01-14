import sys
import os
from subprocess import Popen
import numpy as np
import math
import json

from tpch_benchmark import TPCHBenchmark
from tpcds_benchmark import TPCDSBenchmark

AVAILABLE_BENCHMARKS = {
  "tpch": TPCHBenchmark(),
  "tpcds": TPCDSBenchmark()
}


def run_benchmark(benchmark, config_name, chunk_size):
  # write clustering config into the config json
  sort_order = benchmark.sort_orders()[config_name]
  with open ("clustering_config.json", "w") as clustering_config_file:
    clustering_config_file.write(json.dumps(sort_order) + "\n")

  import os  
  process_env = os.environ.copy()
  process_env["BENCHMARK_TO_RUN"] = benchmark.pretty_name()
  process_env["CLUSTERING_ALGORITHM"] = "DisjointClusters"
  #process_env["CLUSTERING_ALGORITHM"] = "Partitioner"

  # not sure if using "--cache_binary_tables" is a good idea.
  # if a second benchmark does not provide new clustering columns, the old clustering will be loaded and preserved from the table cache.
  # this might influence subsequent benchmarks (e.g. via pruning)
  p = Popen(
            ["/home/Alexander.Loeser/mastersthesis/example_plugin/build-release/hyrisePlayground", "/home/Alexander.Loeser/mastersthesis/example_plugin/build-release/libDriver.so"],
            env=process_env,
            stdout=sys.stdout,
            stdin=sys.stdin,
            stderr=sys.stderr
        )
  p.wait()

  
  generated_stats_path = f"{benchmark.pretty_name()}__SF_{benchmark.scale()}.000000__RUNS_{benchmark.max_runs()}__TIME_{benchmark.time()}__ENCODING_DictionaryFSBA"
  stats_path = f"stats/final/{benchmark.name()}/sf{benchmark.scale()}-2d"

  if not os.path.exists(stats_path):
    os.makedirs(stats_path)
  
  # delete old statistics for this configuration
  p = Popen(
        ["rm", "-r", stats_path + "/" + config_name]
      )
  p.wait()

  os.rename(generated_stats_path, stats_path + "/" + config_name)


def build_sort_order_string(sort_order_per_table):
  return json.dumps(sort_order_per_table)

  table_entries = []
  for table, sort_order in sort_order_per_table.items():
    table_entries.append(table + ":" + ",".join(sort_order))
  return ";".join(table_entries)

def benchmarks_to_run():
  benchmark_names = sys.argv[1:]
  if not len(benchmark_names) > 0:
    print("Error: you need to provide at least one benchmark name")
    exit(1)

  for name in benchmark_names:
    if not name.lower() in AVAILABLE_BENCHMARKS:
      print(f"Error: unknown benchmark: {name}")
      exit(1)

  return [AVAILABLE_BENCHMARKS[name] for name in benchmark_names]

def main():
  benchmarks = benchmarks_to_run()
  for benchmark in benchmarks:
    benchmark_name = benchmark.name()
    print(f"Running benchmarks for {benchmark_name.upper()}")
    num_benchmarks = len(benchmark.sort_orders())
    print(f"Found {num_benchmarks} configurations for {len(benchmark.chunk_sizes())} chunk size(s)")
    for chunk_size in benchmark.chunk_sizes():
      
      # using cached tables is deactivated for now
      # different chunk sizes need different caches, hidden behind symlinks      
      #os.system(f"rm -f {benchmark_name}_cached_tables")
      #os.system(f"ln -s {benchmark_name}_cached_tables_cs{chunk_size} {benchmark_name}_cached_tables")      
      for run_id, config_name in enumerate(benchmark.sort_orders()):
        print(f"Running benchmark {run_id + 1} out of {num_benchmarks}")           
        run_benchmark(benchmark, config_name, chunk_size)


if __name__ == "__main__":
  main()