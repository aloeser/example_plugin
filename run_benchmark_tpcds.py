import sys
import os
from subprocess import Popen
from multiprocessing import Pool
import numpy as np
import math

EXEC_PATH = "./build-release/hyriseBenchmarkTPCDS"
RESULT_PATH = "./benchmark_results/tpcds_time"
RUNS = 30
TIME = 45 # seconds
SCALE = 1
CHUNK_SIZES = [10000, 25000, 100000]
CHUNK_SIZES = [25000, 100000]

SORT_ORDERS = {
  "nosort": {},
  "cd_education_status": {
    "customer_demographics": ["cd_education_status"]
  },  
  #"ss_net_profit": {
  #  "store_sales": ["ss_net_profit"]
  #},
  #"ca_state": {
  #  "customer_address": ["ca_state"]
  #},
  "ss_2d": {    
    "store_sales": ["ss_net_profit", "ss_quantity"]
  },
  "ss_2d_2": {
    "store_sales": ["ss_quantity", "ss_net_profit"]
  },
  "cd_2d": {
    "customer_demographics": ["cd_education_status", "cd_marital_status"]
  },
  "cd_2d_2": {
    "customer_demographics": ["cd_marital_status", "cd_education_status"]
  },
  #"ss_aggregate": {
  #  "store_sales": ['ss_customer_sk', 'ss_item_sk']
  #},
  #"cs_aggregate": {
  #  "catalog_sales": ["cs_bill_customer_sk", "cs_item_sk"]
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
    os.system("rm -f tpcds_cached_tables")
    os.system(f"ln -s tpcds_cached_tables_cs{chunk_size} tpcds_cached_tables")
    
    for config_name in SORT_ORDERS:            
      run_benchmark(config_name, chunk_size)


if __name__ == "__main__":
  main()