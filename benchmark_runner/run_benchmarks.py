import sys
import os
from subprocess import Popen
import numpy as np
import math

from tpch_benchmark import TPCHBenchmark
from tpcds_benchmark import TPCDSBenchmark

AVAILABLE_BENCHMARKS = {
  "tpch": TPCHBenchmark(),
  "tpcds": TPCDSBenchmark()
}


def run_benchmark(benchmark, config_name, chunk_size):
  table_sort_order = build_sort_order_string(benchmark.sort_orders()[config_name])
  process_env = os.environ.copy()
  process_env["TABLE_SORT_ORDER"] = table_sort_order

  p = Popen(
            [benchmark.exec_path(), "--cache_binary_tables", "--sql_metrics", "--time", str(benchmark.time()), "--scale", str(benchmark.scale()), "--chunk_size", str(chunk_size), "--output", f"{benchmark.result_path()}/{config_name}_{chunk_size}.json"],
            env=process_env,
            stdout=sys.stdout,
            stdin=sys.stdin,
            stderr=sys.stderr
        )
  p.wait()

def build_sort_order_string(sort_order_per_table):  
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
    for chunk_size in benchmark.chunk_sizes():
      # different chunk sizes need different caches, hidden behind symlinks      
      os.system(f"rm -f {benchmark_name}_cached_tables")
      os.system(f"ln -s {benchmark_name}_cached_tables_cs{chunk_size} {benchmark_name}_cached_tables")
      
      for config_name in benchmark.sort_orders():            
        run_benchmark(benchmark, config_name, chunk_size)


if __name__ == "__main__":
  main()