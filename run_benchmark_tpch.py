import sys
import os
from subprocess import Popen
from multiprocessing import Pool
import numpy as np
import math

EXEC_PATH = './build-release/hyriseBenchmarkTPCH'
RESULT_PATH = './benchmark_results/tpch_correct_chunksizes2'
RUNS = 30
TIME = 45 # seconds
SCALE = 1
#CHUNK_SIZES = [10000, 25000, 100000]
#CHUNK_SIZES = [25000, 100000]
CHUNK_SIZES = [25000]

SORT_ORDERS = {
  #"nosort": {},
  "default": {
    "lineitem": ["l_shipdate"],
    "orders": ["o_orderdate"]
  },
  #"highest_1d_gain": {
  #  "customer": ["c_mktsegment"],
  #  "lineitem": ["l_receiptdate"],
  #  "orders": ["o_orderdate"],
  #  "part": ["p_brand"],
  #  "partsupp": ["ps_suppkey"]
  #},
  "lineitem_2d_gain": {
    "lineitem": ["l_shipdate", "l_receiptdate"],
    "orders": ["o_orderdate"]
  },
  "q11_test": {
    "lineitem": ["l_shipdate", "l_suppkey"],
    "orders": ["o_orderdate"],
    "partsupp": ["ps_suppkey"]
  },
  "q11_test_2": {
    "lineitem": ["l_suppkey", "l_shipdate"],
    "orders": ["o_orderdate"],
    "partsupp": ["ps_suppkey"]
  },
  #"l_aggregate": {
  #  "lineitem": ["l_shipdate", "l_orderkey"],
  #  "orders": ["o_orderdate"]
  #},
  "q6_discount": {
    "lineitem": ["l_shipdate", "l_discount"],
    "orders": ["o_orderdate"]
  },
  "q6_quantity": {
    "lineitem": ["l_shipdate", "l_quantity"],
    "orders": ["o_orderdate"]
  },
  #"q6_discount_2": {
  #  "lineitem": ["l_discount", "l_shipdate"],
  #  "orders": ["o_orderdate"]
  #},
  #"q6_quantity_2": {
  #  "lineitem": ["l_quantity", "l_shipdate"],
  #  "orders": ["o_orderdate"]
  #},
  #"l_suppkey_2d": {
  #  "lineitem": ["l_shipdate", "l_suppkey"],
  #  "orders": ["o_orderdate"]
  #},
  #"l_suppkey_2d_2": {
  #  "lineitem": ["l_suppkey", "l_shipdate"],
  #  "orders": ["o_orderdate"]
  #},
  #"l_suppkey_commit": {
  #  "lineitem": ["l_commitdate", "l_suppkey"],
  #  "orders": ["o_orderdate"]
  #},
  #"l_suppkey_commit_2": {
  #  "lineitem": ["l_suppkey", "l_commitdate"],
  #  "orders": ["o_orderdate"]
  #},
}



def run_benchmark(config_name, chunk_size):
  table_sort_order = build_sort_order_string(SORT_ORDERS[config_name])
  process_env = os.environ.copy()
  process_env["TABLE_SORT_ORDER"] = table_sort_order

  p = Popen(
            [EXEC_PATH, "--cache_binary_tables", "--time", str(TIME), "--scale", str(SCALE), "--chunk_size", str(chunk_size), "--output", f"{RESULT_PATH}/{config_name}_{chunk_size}.json"],
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

def main():
  for chunk_size in CHUNK_SIZES:
    # different chunk sizes need different caches, hidden behind symlinks      
    os.system("rm -f tpch_cached_tables")
    os.system(f"ln -s tpch_cached_tables_cs{chunk_size} tpch_cached_tables")
    
    for config_name in SORT_ORDERS:            
      run_benchmark(config_name, chunk_size)


if __name__ == "__main__":
  main()