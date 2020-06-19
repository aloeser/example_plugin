#include <fstream>

#include "plan_cache_csv_exporter.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"

#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"

namespace opossum {

PlanCacheCsvExporter::PlanCacheCsvExporter(const std::string export_folder_name) : _sm{Hyrise::get().storage_manager}, _export_folder_name{export_folder_name}, _table_scans{}, _projections{} {
  std::ofstream joins_csv;
  std::ofstream validates_csv;
  std::ofstream aggregates_csv;

  joins_csv.open(_export_folder_name + "/joins.csv");
  validates_csv.open(_export_folder_name + "/validates.csv");
  aggregates_csv.open(_export_folder_name + "/aggregates.csv");

  joins_csv << "QUERY_HASH,JOIN_MODE,LEFT_TABLE_NAME,LEFT_COLUMN_NAME,LEFT_TABLE_ROW_COUNT,RIGHT_TABLE_NAME,RIGHT_COLUMN_NAME,RIGHT_TABLE_ROW_COUNT,OUTPUT_ROWS,PREDICATE_COUNT,PRIMARY_PREDICATE,RUNTIME_NS,MATERIALIZE,CLUSTER,BUILD,PROBE,WRITE_OUTPUT,DESCRIPTION,BUILD_TABLE,BUILD_COLUMN,BUILD_TABLE_SIZE,PROBE_TABLE,PROBE_COLUMN,PROBE_TABLE_SIZE,BUILD_SORTED,PROBE_SORTED\n";
  validates_csv << "QUERY_HASH,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS\n";
  aggregates_csv << "QUERY_HASH,AGGREGATE_HASH,COLUMN_TYPE,TABLE_NAME,COLUMN_NAME,GROUP_BY_COLUMN_COUNT,AGGREGATE_COLUMN_COUNT,INPUT_ROWS,OUTPUT_ROWS,RUNTIME_NS,DESCRIPTION\n";

  joins_csv.close();
  validates_csv.close();
  aggregates_csv.close();
}

void PlanCacheCsvExporter::run() {
  std::ofstream plan_cache_csv_file(_export_folder_name + "/plan_cache.csv");
  plan_cache_csv_file << "QUERY_HASH|EXECUTION_COUNT|QUERY_STRING\n";

  // for (const auto& [query_string, physical_query_plan] : *SQLPipelineBuilder::default_pqp_cache) {
  for (auto iter = Hyrise::get().default_pqp_cache->unsafe_begin(); iter != Hyrise::get().default_pqp_cache->unsafe_end(); ++iter) {
    const auto& [query_string, physical_query_plan] = *iter;
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    _process_pqp(physical_query_plan, query_hex_hash.str(), visited_pqp_nodes);

    // Plan cache CSV
    auto& gdfs_cache = dynamic_cast<GDFSCache<std::string, std::shared_ptr<AbstractOperator>>&>(Hyrise::get().default_pqp_cache->unsafe_cache());
    const size_t frequency = gdfs_cache.frequency(query_string);

    auto query_single_line{query_string};
    query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
                            query_single_line.end());
    plan_cache_csv_file << "\"" << query_hex_hash.str() << "\"" << "|" << frequency << "|\"" << query_single_line << "\"\n";
  }

  write_to_disk();
}

void PlanCacheCsvExporter::write_to_disk() const {
  auto write_lines = [](const auto file_name, const auto operator_information) {
    const auto separator = "|";

    std::ofstream output_csv;
    output_csv.open(file_name);
    output_csv << operator_information.csv_header << "\n";
    for (const auto& instance : operator_information.instances) {
      const auto string_vector = instance.string_vector();
      for (auto index = size_t{0}; index < string_vector.size(); ++index) {
        output_csv << string_vector[index];
        if (index < (string_vector.size() - 1)) {
          output_csv << separator;
        }
      }
      output_csv << "\n";
    }
    output_csv.close();
  };

  write_lines(_export_folder_name + "/table_scans.csv", _table_scans);
  write_lines(_export_folder_name + "/projections.csv", _projections);
}

std::string PlanCacheCsvExporter::_process_join(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto node = op->lqp_node;
  const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

  const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
                                                                         *node->left_input(), *node->right_input());

  std::stringstream ss;
  ss << query_hex_hash << "," << join_mode_to_string.left.at(join_node->join_mode) << ",";
  if (operator_predicate.has_value()) {
    const auto column_expressions = join_node->column_expressions();
    const auto predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);
    std::string table_name_0, table_name_1;
    ColumnID original_column_id_0, original_column_id_1;

      // if (*column_expression == *(predicate_expression->arguments[0])) {
      {
        const auto expression = predicate_expression->arguments[0];
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(expression);
          original_column_id_0 = column_expression->original_column_id;

          const auto original_node_0 = column_expression->original_node.lock();
          if (original_node_0->type == LQPNodeType::StoredTable) {
            const auto stored_table_node_0 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_0);
            table_name_0 = stored_table_node_0->table_name;
          }
        }
      }
      // }

      // if (*column_expression == *(predicate_expression->arguments[1])) {
      {
        const auto expression = predicate_expression->arguments[1];
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<const LQPColumnExpression>(expression);
          original_column_id_1 = column_expression->original_column_id;
          
          const auto original_node_1 = column_expression->original_node.lock();
          if (original_node_1->type == LQPNodeType::StoredTable) {
            const auto stored_table_node_1 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_1);
            table_name_1 = stored_table_node_1->table_name;
          }
        }
      }
      // }

      const auto& perf_data = op->performance_data;

      std::string column_name_0, column_name_1;
      // In cases where the join partner is not a column, we fall back to empty column names.
      // Exemplary query is the rewrite of TPC-H Q2 where `min(ps_supplycost)` is joined with `ps_supplycost`.
      if (table_name_0 != "") {
        const auto sm_table_0 = _sm.get_table(table_name_0);
        column_name_0 = sm_table_0->column_names()[original_column_id_0];
      }
      if (table_name_1 != "") {
        const auto sm_table_1 = _sm.get_table(table_name_1);
        column_name_1 = sm_table_1->column_names()[original_column_id_1];
      }

      // auto table_id = _table_name_id_map.left.at(table_name_0);
      // auto identifier = std::make_pair(table_id, original_column_id_0);

      if (operator_predicate->is_flipped()) {
        std::swap(table_name_0, table_name_1);
        std::swap(column_name_0, column_name_1);
      }

      const auto left_table_row_count = _output_size(op->input_left());
      const auto right_table_row_count = _output_size(op->input_right());

      ss << table_name_0 << "," << column_name_0 << "," << left_table_row_count  << ",";
      ss << table_name_1 << "," << column_name_1 << "," << right_table_row_count << ",";

      ss << perf_data->output_row_count << ",";
      ss << join_node->node_expressions.size() << "," << predicate_condition_to_string.left.at((*operator_predicate).predicate_condition) << ",";
      ss << perf_data->walltime.count();
      // update_map(join_map, identifier, perf_data);

      const auto step_perf_data = static_cast<StepOperatorPerformanceData*>(perf_data.get());
      if (step_perf_data) {
        for (size_t step = 0; step < 5; step++) {
          ss << "," << step_perf_data->step_runtimes[step].count();
        }
      } else {
        ss << ",0,0,0,0,0";
      }
      ss << "," << op->description() << ",";

      if (op->type() == OperatorType::JoinHash) {
        // determine probe and build column
        bool probe_side_is_left = true;
        const auto join_op = std::dynamic_pointer_cast<const AbstractJoinOperator>(op);
        const auto& mode = join_op->mode();
        if (mode == JoinMode::Semi || mode == JoinMode::Left || mode == JoinMode::AntiNullAsFalse || mode == JoinMode::AntiNullAsTrue) {
          probe_side_is_left = true;
        } else if (mode == JoinMode::Right) {
          probe_side_is_left = false;
        } else if (mode == JoinMode::Inner) {
          if (left_table_row_count > right_table_row_count) {
            probe_side_is_left = true;
          } else {
            probe_side_is_left = false;
          }
        }

        std::string probe_table;
        std::string probe_column;
        size_t probe_table_size;
        std::string build_table;
        std::string build_column;
        size_t build_table_size;

        if (probe_side_is_left) {
          probe_table = table_name_0;
          probe_column = column_name_0;
          build_table = table_name_1;
          build_column = column_name_1;
          probe_table_size = left_table_row_count;
          build_table_size = right_table_row_count;
        } else {
          probe_table = table_name_1;
          probe_column = column_name_1;
          build_table = table_name_0;
          build_column = column_name_0;
          probe_table_size = right_table_row_count;
          build_table_size = left_table_row_count;
        }

        ss << build_table << "," << build_column << "," << build_table_size << "," << probe_table << "," << probe_column << "," << probe_table_size << ",";

        // determine whether build or probe column could arrive sorted
        // sortedness is kept by TableScans, Validates, (Projection, Alias), and SemiJoins with radix_bits = 0
        bool probe_column_propagates_sortedness = false;
        bool build_column_propagates_sortedness = false;

        if (probe_side_is_left) {
          probe_column_propagates_sortedness = _propagates_sortedness(op->input_left());
          build_column_propagates_sortedness = _propagates_sortedness(op->input_right());
        } else {
          probe_column_propagates_sortedness = _propagates_sortedness(op->input_right());
          build_column_propagates_sortedness = _propagates_sortedness(op->input_left());
        }

        ss << build_column_propagates_sortedness << "," << probe_column_propagates_sortedness << "\n";
      } else {
        // not a hash join -> probe/build terms do not make sense
        ss << ",,0,,,0,0,0\n";
      }

      // How do we know whether the left_input_rows are actually added to the left table?
      // table_id = _table_name_id_map.left.at(table_name_1);
      // identifier = std::make_pair(table_id, original_column_id_1);
      // update_map(join_map, identifier, perf_data, false);
  } else {
    ss << "UNEXPECTED join operator_predicate.has_value()";
    std::cout << op << std::endl;
  }

  return ss.str();
}


void PlanCacheCsvExporter::_process_index_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);
  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate(), *node);

  // TODO(anyone): I never used an index scan, so I am not sure if I am handling it right.
  std::cout << "Unhandled operation ..." << std::endl;
  if (operator_predicates->size() < 2) {
    for (const auto& el : node->node_expressions) {
      visit_expression(el, [&](const auto& expression) {
        if (expression->type == ExpressionType::LQPColumn) {
          const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
          const auto original_node = column_expression->original_node.lock();

          if (original_node->type == LQPNodeType::StoredTable) {
            const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
            const auto& table_name = stored_table_node->table_name;
            // const auto table_id = _table_name_id_map.left.at(table_name);

            std::cout << table_name << "," << _output_size(op->input_left()) << std::endl;
          }
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }
}

bool PlanCacheCsvExporter::_propagates_sortedness(const std::shared_ptr<const AbstractOperator>& op) const {
  bool propagates = true;
  Assert(op, "operator was null");

  auto current_operator = op;

  // loop ends when we have reached the first node
  while (current_operator->input_left()) {
    const auto& type = current_operator->type();
    if (type == OperatorType::JoinHash) {
      const auto join_op = std::dynamic_pointer_cast<const JoinHash>(current_operator);
      Assert(join_op, "Bug");

      const auto& mode = join_op->mode();
      if (mode != JoinMode::Semi && mode != JoinMode::AntiNullAsFalse && mode != JoinMode::AntiNullAsTrue) {
        propagates = false;
        break;
      }

      if (!join_op->_radix_bits || *join_op->_radix_bits > 0) {
        propagates = false;
        break;
      }
    } else if (type == OperatorType::TableScan || type == OperatorType::Validate || type == OperatorType::Projection || type == OperatorType::Alias) {
        // nothing to do, those operators propagate sorted data
    } else {
      propagates = false;
      break;
    }
    current_operator = current_operator->input_left();
  }

  return propagates;
}

void PlanCacheCsvExporter::_process_table_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleTableScan> table_scans;

  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

  const auto predicate = predicate_node->predicate();

  const auto& perf_data = op->performance_data;

  auto description = op->description();
  description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
  description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

  // ugly hack to avoid queries on temporary columns
  // skip ColumnVsColumn scans as well, as they cant be pruned
  std::vector<std::string> forbidden_words = {"ColumnVsColumn", "SUBQUERY", "SUM", "AVG", "COUNT"};

  for (const auto& forbidden_word : forbidden_words) {
    if (description.find(forbidden_word) != std::string::npos)
      return;
  }

  std::cout << "trying to re-execute " << description << std::endl;

  auto copied_scan = op->deep_copy();
  auto copied_table = copied_scan->input_left();
  Assert(copied_table, "we cannot have no input");


  while (copied_table->input_left()) {
    // we are not interested in predicates after joins
    if (copied_table->type() == OperatorType::JoinHash ||
        copied_table->type() == OperatorType::JoinIndex ||
        copied_table->type() == OperatorType::JoinNestedLoop ||
        copied_table->type() == OperatorType::JoinSortMerge ||
        copied_table->type() == OperatorType::JoinVerification) {
      std::cout << "SKIPPED because the scan happened on a " << copied_table->description() << std::endl;
      return;
    }
    std::cout << "walking down the pqp, ignoring a " << copied_table->description() << std::endl;
    copied_table = copied_table->input_left();
  }
  Assert(copied_table, "that went too far down the pqp");
  auto input_table = std::const_pointer_cast<AbstractOperator>(copied_table);

  input_table->reset_transaction_context();
  Assert(!input_table->transaction_context_is_set(), "should not have a transaction context");
  input_table->execute();

  const auto original_get_table = _get_table_operator_for_table_scan(op);
  const void * address = static_cast<const void*>(original_get_table.get());
  std::stringstream address_stream;
  address_stream << address;
  const auto get_table_pointer_string = address_stream.str();

  copied_scan->set_input_left(input_table);
  copied_scan->reset_transaction_context();
  Assert(!copied_scan->transaction_context_is_set(), "should not have a transaction context");
  copied_scan->execute();
  std::cout << "SUCCESS" << std::endl;

  const auto& copied_scan_perf_data = copied_scan->performance_data;


  // We iterate through the expression until we find the desired column being scanned. This works acceptably ok
  // for most scans we are interested in (e.g., visits both columns of a column vs column scan).
  visit_expression(predicate, [&](const auto& expression) {
    std::string column_type{};
    if (expression->type == ExpressionType::LQPColumn) {
      const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      const auto original_node = column_expression->original_node.lock();

      // Check if scan on data or reference table (this should ignore scans of temporary columns)
      if (original_node->type == LQPNodeType::StoredTable) {
        
        if (original_node == node->left_input()) {
          column_type = "DATA";
        } else {
          column_type = "REFERENCE";
        }

        const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
        const auto& table_name = stored_table_node->table_name;

        const auto original_column_id = column_expression->original_column_id;
        const auto sm_table = _sm.get_table(table_name);
        std::string column_name = "";
        if (original_column_id != INVALID_COLUMN_ID) {
          column_name = sm_table->column_names()[original_column_id];
        } else {
          column_name = "COUNT(*)";
        }

        table_scans.emplace_back(SingleTableScan{query_hex_hash, column_type, table_name, column_name,
                             _output_size(op->input_left()), perf_data->output_row_count, static_cast<size_t>(perf_data->walltime.count()),
                             description, _output_size(copied_scan->input_left()), copied_scan_perf_data->output_row_count,
                             static_cast<size_t>(copied_scan_perf_data->walltime.count()), get_table_pointer_string});
      }
    }
    return ExpressionVisitation::VisitArguments;
  });

  _table_scans.instances.insert(_table_scans.instances.end(), table_scans.begin(), table_scans.end());
}

std::string PlanCacheCsvExporter::_process_validate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto& perf_data = op->performance_data;

  std::stringstream ss;
  ss << query_hex_hash << "," << _output_size(op->input_left()) << "," << perf_data->output_row_count << "," << perf_data->walltime.count() << "\n";

  return ss.str();
}

size_t PlanCacheCsvExporter::_output_size(const std::shared_ptr<const AbstractOperator>& op) const {
  Assert(op, "op is nullptr");
  Assert(op->performance_data, "op->performance_data is nullptr");
  Assert(op->performance_data->has_output, "op has no output");
  return op->performance_data->output_row_count;
}

std::string PlanCacheCsvExporter::_process_aggregate(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto node = op->lqp_node;
  const auto aggregate_node = std::dynamic_pointer_cast<const AggregateNode>(node);

  std::stringstream ss;
  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream agg_hex_hash;
  agg_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());

  for (const auto& el : aggregate_node->node_expressions) {
    // TODO: ensure we do not traverse too deep here, isn't the loop sufficient?
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto original_node = column_expression->original_node.lock();

        if (original_node->type == LQPNodeType::StoredTable) {
          ss << query_hex_hash << "," << agg_hex_hash.str() << ",";
          if (original_node == node->left_input()) {
            ss << "DATA,";
          } else {
            ss << "REFERENCE,";
          }

          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& table_name = stored_table_node->table_name;
          ss << table_name << ",";

          const auto original_column_id = column_expression->original_column_id;
          const auto& perf_data = op->performance_data;

          const auto node_expression_count = aggregate_node->node_expressions.size();
          const auto group_by_column_count = aggregate_node->aggregate_expressions_begin_idx;
          ss << group_by_column_count << "," << (node_expression_count - group_by_column_count) << ",";

          const auto sm_table = _sm.get_table(table_name);
          std::string column_name = "";
          if (original_column_id != INVALID_COLUMN_ID) {
            column_name = sm_table->column_names()[original_column_id];
          } else {
            column_name = "COUNT(*)";
          }
          ss << column_name << "," << _output_size(op->input_left()) << ",";
          ss << perf_data->output_row_count << ",";
          ss << perf_data->walltime.count() << ",\"";
          ss << op->description() << "\"\n";
        }
      }
      return ExpressionVisitation::VisitArguments;
    });
  }

  return ss.str();
}

void PlanCacheCsvExporter::_process_projection(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleProjection> projections;

  const auto node = op->lqp_node;
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream proj_hex_hash;
  proj_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());
  const auto proj_hex_hash_str = proj_hex_hash.str();

  for (const auto& el : projection_node->node_expressions) {
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto original_node = column_expression->original_node.lock();

        if (original_node->type == LQPNodeType::StoredTable) {
          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& table_name = stored_table_node->table_name;
          const auto original_column_id = column_expression->original_column_id;
          const std::string column_type = (original_node == node->left_input()) ? "DATA" : "REFERENCE";
          const auto& perf_data = op->performance_data;
          const auto sm_table = _sm.get_table(table_name);
          std::string column_name = "";
          if (original_column_id != INVALID_COLUMN_ID) {
            column_name = sm_table->column_names()[original_column_id];
          } else {
            column_name = "COUNT(*)";
          }
          auto description = op->lqp_node->description();
          description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
          description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

          projections.emplace_back(SingleProjection{query_hex_hash, proj_hex_hash_str, column_type, table_name, column_name, _output_size(op->input_left()),
            perf_data->output_row_count, static_cast<size_t>(perf_data->walltime.count()), description});
        }
      }
      // TODO: does that help???
      // else {
      //   if (expression->type == ExpressionType::Function) {
      //     std::cout << "Function " << expression->as_column_name() << " - " << expression->arguments.size() << std::endl;
      //     for (const auto& t : expression->arguments) {
      //       if (t->type == ExpressionType::LQPColumn) {
      //         std::cout << "#" << t->as_column_name() << std::endl;
      //       }
      //     }
      //   }
      //   if (expression->type == ExpressionType::Arithmetic) {
      //     std::cout << "Arithmetic " << expression->as_column_name() << " - " << expression->arguments.size() << std::endl;
      //     for (const auto& t : expression->arguments) {
      //       if (t->type == ExpressionType::LQPColumn) {
      //         std::cout << "#" << t->as_column_name() << std::endl;
      //       }
      //     }
      //   }
      //   stringstreams[visit_write_id] += query_hex_hash + "," + proj_hex_hash_str;
      //   stringstreams[visit_write_id] += ",CALC_PROJECTION,";
      //   stringstreams[visit_write_id] += ",,,,,\"[Projection] Calculation: ";
      //   stringstreams[visit_write_id] += expression->as_column_name() + "\"";
      // }
      return ExpressionVisitation::VisitArguments;
    });
  }

  _projections.instances.insert(_projections.instances.end(), projections.begin(), projections.end());
}

// Only call this function on table scans that are not based on a join
const std::shared_ptr<const AbstractOperator> PlanCacheCsvExporter::_get_table_operator_for_table_scan(const std::shared_ptr<const AbstractOperator> table_scan) const {
  Assert(table_scan->type() == OperatorType::TableScan, "not a TableScan");

  auto op = table_scan;
  while (op->input_left()) {
    op = op->input_left();
  }

  Assert(op->type() == OperatorType::GetTable, "not a GetTable");
  return op;
}

void PlanCacheCsvExporter::_process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
                                        std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes) {
  std::ofstream joins_csv;
  std::ofstream validates_csv;
  std::ofstream aggregates_csv;

  joins_csv.open(_export_folder_name + "/joins.csv", std::ios_base::app);
  validates_csv.open(_export_folder_name + "/validates.csv", std::ios_base::app);
  aggregates_csv.open(_export_folder_name + "/aggregates.csv", std::ios_base::app);

  // TODO(anyone): handle diamonds?
  // Todo: handle index scans
  if (op->type() == OperatorType::TableScan) {
    _process_table_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::JoinHash || op->type() == OperatorType::JoinNestedLoop || op->type() == OperatorType::JoinSortMerge) {
    joins_csv << _process_join(op, query_hex_hash);
  } else if (op->type() == OperatorType::IndexScan) {
    _process_index_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::Validate) {
    validates_csv << _process_validate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Aggregate) {
    aggregates_csv << _process_aggregate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Projection) {
    //_process_projection(op, query_hex_hash);
  } else {
  }

  visited_pqp_nodes.insert(op);

  const auto left_input = op->input_left();
  const auto right_input = op->input_right();
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    _process_pqp(left_input, query_hex_hash, visited_pqp_nodes);
    visited_pqp_nodes.insert(std::move(left_input));
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    _process_pqp(right_input, query_hex_hash, visited_pqp_nodes);
    visited_pqp_nodes.insert(std::move(right_input));
  }
}

}  // namespace opossum 
