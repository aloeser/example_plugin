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
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/table.hpp"

namespace {

using namespace opossum;  // NOLINT

std::string camel_to_csv_row_title(const std::string& title) {
  auto result = std::string{};
  auto string_index = size_t{0};
  for (unsigned char character : title) {
    if (string_index > 0 && std::isupper(character)) {
      result += '_';
      result += character;
      ++string_index;
      continue;
    }
    result += std::toupper(character);
    ++string_index;
  }
  return result;
}

std::string get_operator_hash(const std::shared_ptr<const AbstractOperator> op) {
  std::stringstream ss;
  std::ostringstream op_description_ostream;
  op_description_ostream << *op;
  std::stringstream description_hex_hash;
  description_hex_hash << std::hex << std::hash<std::string>{}(op_description_ostream.str());
  ss << op << description_hex_hash.str();

  return ss.str();
}

}  // namespace

namespace opossum {

PlanCacheCsvExporter::PlanCacheCsvExporter(const std::string export_folder_name) : _sm{Hyrise::get().storage_manager}, _export_folder_name{export_folder_name}, _table_scans{}, _projections{} {
  std::ofstream joins_csv;
  std::ofstream general_operators_csv;
  std::ofstream aggregates_csv;

  joins_csv.open(_export_folder_name + "/joins.csv");

  general_operators_csv.open(_export_folder_name + "/general_operators.csv");
  aggregates_csv.open(_export_folder_name + "/aggregates.csv");

  joins_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|JOIN_MODE|LEFT_TABLE_NAME|LEFT_COLUMN_NAME|LEFT_TABLE_CHUNK_COUNT|LEFT_TABLE_ROW_COUNT|LEFT_COLUMN_TYPE|RIGHT_TABLE_NAME|RIGHT_COLUMN_NAME|RIGHT_TABLE_CHUNK_COUNT|RIGHT_TABLE_ROW_COUNT|RIGHT_COLUMN_TYPE|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|PREDICATE_COUNT|PRIMARY_PREDICATE|IS_FLIPPED|RADIX_BITS|";

  for (const auto step_name : magic_enum::enum_names<JoinHash::OperatorSteps>()) {
    const auto step_name_str = std::string{step_name};
    joins_csv << camel_to_csv_row_title(step_name_str) << "_NS|";
  }

  joins_csv << "RUNTIME_NS|DESCRIPTION|";
  joins_csv << "PROBE_SORTED|BUILD_SORTED|PROBE_SIDE|BUILD_SIDE\n";

  aggregates_csv.open(_export_folder_name + "/aggregates.csv");
  aggregates_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|COLUMN_TYPE|TABLE_NAME|COLUMN_NAME|GROUP_BY_COLUMN_COUNT|AGGREGATE_COLUMN_COUNT|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|";

  general_operators_csv << "OPERATOR_TYPE|QUERY_HASH|OPERATOR_HASH|LEFT_INPUT_OPERATOR_HASH|RIGHT_INPUT_OPERATOR_HASH|INPUT_CHUNK_COUNT|INPUT_ROW_COUNT|OUTPUT_CHUNK_COUNT|OUTPUT_ROW_COUNT|RUNTIME_NS\n";

  for (const auto step_name : magic_enum::enum_names<AggregateHash::OperatorSteps>()) {
    const auto step_name_str = std::string{step_name};
    aggregates_csv << camel_to_csv_row_title(step_name_str) << "_NS|";
  }
  aggregates_csv << "RUNTIME_NS|DESCRIPTION\n";

  joins_csv.close();
  general_operators_csv.close();
  aggregates_csv.close();
}

void PlanCacheCsvExporter::run() {
  std::ofstream plan_cache_csv_file(_export_folder_name + "/plan_cache.csv");
  plan_cache_csv_file << "QUERY_HASH|EXECUTION_COUNT|QUERY_STRING\n";

  const auto cache_snapshot = Hyrise::get().default_pqp_cache->snapshot();
  for (const auto& [query_string, snapshot_entry] : cache_snapshot) {
    const auto& physical_query_plan = snapshot_entry.value;
    const auto& frequency = snapshot_entry.frequency;
    Assert(frequency, "Optional frequency is unexpectedly not set.");

    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    _process_pqp(physical_query_plan, query_hex_hash.str(), visited_pqp_nodes);

    auto query_single_line{query_string};
    query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
                            query_single_line.end());

    plan_cache_csv_file << "\"" << query_hex_hash.str() << "\"" << "|" << *frequency << "|\"" << query_single_line << "\"\n";
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

  write_lines(_export_folder_name + "/get_tables.csv", _get_tables);
  write_lines(_export_folder_name + "/table_scans.csv", _table_scans);
  write_lines(_export_folder_name + "/projections.csv", _projections);
}

std::string PlanCacheCsvExporter::_process_join(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto node = op->lqp_node;
  const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

  const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
                                                                         *node->left_input(), *node->right_input());

  std::stringstream ss;
  ss << "JOIN|" << query_hex_hash << "|" << get_operator_hash(op) << "|" << get_operator_hash(op->left_input()) << "|"
     << get_operator_hash(op->right_input()) << "|" << join_mode_to_string.left.at(join_node->join_mode) << "|";
  if (operator_predicate.has_value()) {
    const auto predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);
    std::string left_table_name{};
    std::string right_table_name{};
    ColumnID left_original_column_id{};
    ColumnID right_original_column_id{};
    std::string left_column_type{};
    std::string right_column_type{};

    const auto first_predicate_expression = predicate_expression->arguments[0];
    if (first_predicate_expression->type == ExpressionType::LQPColumn) {
      const auto left_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(first_predicate_expression);
      left_original_column_id = left_column_expression->original_column_id;

      const auto original_node_0 = left_column_expression->original_node.lock();
      if (original_node_0->type == LQPNodeType::StoredTable) {
        const auto stored_table_node_0 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_0);
        left_table_name = stored_table_node_0->table_name;

        if (original_node_0 == node->left_input()) {
          left_column_type = "DATA";
        } else {
          left_column_type = "REFERENCE";
        }
      }
    }

    const auto second_predicate_expression = predicate_expression->arguments[1];
    if (second_predicate_expression->type == ExpressionType::LQPColumn) {
      const auto right_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(second_predicate_expression);
      right_original_column_id = right_column_expression->original_column_id;
      
      const auto original_node_1 = right_column_expression->original_node.lock();
      if (original_node_1->type == LQPNodeType::StoredTable) {
        const auto stored_table_node_1 = std::dynamic_pointer_cast<const StoredTableNode>(original_node_1);
        right_table_name = stored_table_node_1->table_name;

        if (original_node_1 == node->right_input()) {
          right_column_type = "DATA";
        } else {
          right_column_type = "REFERENCE";
        }
      }
    }

    std::string column_name_0, column_name_1;
    // In cases where the join partner is not a column, we fall back to empty column names.
    // Exemplary query is the rewrite of TPC-H Q2 where `min(ps_supplycost)` is joined with `ps_supplycost`.
    if (left_table_name != "") {
      const auto sm_table_0 = _sm.get_table(left_table_name);
      column_name_0 = sm_table_0->column_names()[left_original_column_id];
    }
    if (right_table_name != "") {
      const auto sm_table_1 = _sm.get_table(right_table_name);
      column_name_1 = sm_table_1->column_names()[right_original_column_id];
    }


    auto description = op->description();
    description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
    description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

    const auto& left_input_perf_data = op->left_input()->performance_data;
    const auto& right_input_perf_data = op->right_input()->performance_data;

    // Check if the join predicate has been switched (hence, it differs between LQP and PQP) which is done when
    // table A and B are joined but the join predicate is "flipped" (e.g., b.x = a.x). The effect of flipping is that
    // the predicates are in the order (left/right) as the join input tables are.
    if (!operator_predicate->is_flipped()) {
      ss << left_table_name << "|" << column_name_0 << "|" << left_input_perf_data->output_chunk_count
         << "|" << left_input_perf_data->output_row_count << "|" << left_column_type << "|";
      ss << right_table_name << "|" << column_name_1 << "|" << right_input_perf_data->output_chunk_count
         << "|" << right_input_perf_data->output_row_count << "|" << right_column_type << "|";
    } else {
      ss << right_table_name << "|" << column_name_1 << "|" << left_input_perf_data->output_chunk_count
         << "|" << left_input_perf_data->output_row_count << "|" << right_column_type << "|";
      ss << left_table_name << "|" << column_name_0 << "|" << right_input_perf_data->output_chunk_count
         << "|" << right_input_perf_data->output_row_count << "|" << left_column_type << "|";
    }



    const auto& perf_data = op->performance_data;    
    ss << perf_data->output_chunk_count << "|" << perf_data->output_row_count << "|";
    ss << join_node->node_expressions.size() << "|" << predicate_condition_to_string.left.at((*operator_predicate).predicate_condition) << "|";


    if (const auto join_hash_op = dynamic_pointer_cast<const JoinHash>(op)) {
      const auto& join_hash_perf_data = dynamic_cast<const JoinHash::PerformanceData&>(*join_hash_op->performance_data);
      

      const auto flipped = join_hash_perf_data.right_input_is_build_side ? "TRUE" : "FALSE";

      ss << flipped << "|" << join_hash_perf_data.radix_bits << "|";

      for (const auto step_name : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
        ss << join_hash_perf_data.get_step_runtime(step_name).count() << "|";
      }

      ss << perf_data->walltime.count() << "|" << description << "|";

      bool probe_column_propagates_sortedness = false;
      bool build_column_propagates_sortedness = false;
      std::string probe_side, build_side;

      if (join_hash_perf_data.right_input_is_build_side) {
        probe_column_propagates_sortedness = _propagates_sortedness(op->left_input());
        build_column_propagates_sortedness = _propagates_sortedness(op->right_input());
        probe_side = "LEFT";
        build_side = "RIGHT";
      } else {
        probe_column_propagates_sortedness = _propagates_sortedness(op->right_input());
        build_column_propagates_sortedness = _propagates_sortedness(op->left_input());
        probe_side = "RIGHT";
        build_side = "LEFT";
      }

      ss << build_column_propagates_sortedness << "|" << probe_column_propagates_sortedness << "|";
      ss << probe_side << "|" << build_side << "\n";
    } else {
      ss << "FALSE|NULL|";
      for (auto index = size_t{0}; index < magic_enum::enum_count<JoinHash::OperatorSteps>(); ++index) {
        ss << "NULL|";
      }
      ss << "0|0|NULL|NULL\n";
    }
  } else {
    ss << "UNEXPECTED join operator_predicate.has_value()";
    std::cout << op << std::endl;
  }

  return ss.str();
}

bool PlanCacheCsvExporter::_propagates_sortedness(const std::shared_ptr<const AbstractOperator>& op) const {
  bool propagates = true;
  Assert(op, "operator was null");

  auto current_operator = op;

  // loop ends when we have reached the first node
  while (current_operator->left_input()) {
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
    current_operator = current_operator->left_input();
  }

  return propagates;
}

void PlanCacheCsvExporter::_process_table_scan(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleTableScan> table_scans;

  const auto node = op->lqp_node;
  const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

  const auto predicate = predicate_node->predicate();

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

  //std::cout << "trying to re-execute " << description << std::endl;
  //auto copied_scan = op->deep_copy();
  //auto copied_table = copied_scan->left_input();
  //Assert(copied_table, "we cannot have no input");


  // Skip predicates that occur after joins
  auto current_op = op->left_input();
  while (current_op->left_input()) {
    // we are not interested in predicates after joins
    if (current_op->type() == OperatorType::JoinHash ||
        current_op->type() == OperatorType::JoinIndex ||
        current_op->type() == OperatorType::JoinNestedLoop ||
        current_op->type() == OperatorType::JoinSortMerge ||
        current_op->type() == OperatorType::JoinVerification) {
      std::cout << "SKIPPED because the scan happened on a " << current_op->description() << std::endl;
      return;
    }
    std::cout << "walking down the pqp, ignoring a " << current_op->description() << std::endl;
    current_op = current_op->left_input();
  }
  Assert(current_op, "that went too far down the pqp");
  //auto input_table = std::const_pointer_cast<AbstractOperator>(copied_table);

  //input_table->reset_transaction_context();
  //Assert(!input_table->transaction_context_is_set(), "should not have a transaction context");
  //input_table->execute();

  const auto original_get_table = _get_table_operator_for_table_scan(op);

  //copied_scan->set_input_left(input_table);
  //copied_scan->reset_transaction_context();
  //Assert(!copied_scan->transaction_context_is_set(), "should not have a transaction context");
  //copied_scan->execute();
  //std::cout << "SUCCESS" << std::endl;

  //const auto& copied_scan_perf_data = copied_scan->performance_data;


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

        auto scan_predicate_condition = std::string{"Unknown"};
        const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, *stored_table_node);
        if (operator_scan_predicates->size() == 1) {
          const auto condition = (*operator_scan_predicates)[0].predicate_condition;
          scan_predicate_condition = magic_enum::enum_name(condition);
        }

        const auto original_column_id = column_expression->original_column_id;
        const auto sm_table = _sm.get_table(table_name);
        std::string column_name = "";
        if (original_column_id != INVALID_COLUMN_ID) {
          column_name = sm_table->column_names()[original_column_id];
        } else {
          column_name = "COUNT(*)";
        }

        const auto table_scan_op = dynamic_pointer_cast<const TableScan>(op);
        Assert(table_scan_op, "Unexpected non-table-scan operators");
        const auto& operator_perf_data = dynamic_cast<const TableScan::PerformanceData&>(*table_scan_op->performance_data);

        auto description = op->description();
        description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
        description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

        const auto& left_input_perf_data = op->left_input()->performance_data;

        table_scans.emplace_back(SingleTableScan{query_hex_hash, get_operator_hash(op),
                                                 get_operator_hash(op->left_input()), "NULL", column_type, table_name,
                                                 column_name, scan_predicate_condition,
                                                 operator_perf_data.num_chunks_with_early_out,
                                                 operator_perf_data.num_chunks_with_binary_search,
                                                 left_input_perf_data->output_chunk_count,
                                                 left_input_perf_data->output_row_count,
                                                 operator_perf_data.output_chunk_count,
                                                 operator_perf_data.output_row_count,
                                                 static_cast<size_t>(operator_perf_data.walltime.count()),
                                                 description, get_operator_hash(original_get_table)});
      }
    }
    return ExpressionVisitation::VisitArguments;
  });

  _table_scans.instances.insert(_table_scans.instances.end(), table_scans.begin(), table_scans.end());
}

void PlanCacheCsvExporter::_process_get_table(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  std::vector<SingleGetTable> get_tables;

  const auto get_table_op = dynamic_pointer_cast<const GetTable>(op);
  Assert(get_table_op, "Processing GetTable operator but another operator was passed.");

  auto description = op->description();
  description.erase(std::remove(description.begin(), description.end(), '\n'), description.end());
  description.erase(std::remove(description.begin(), description.end(), '"'), description.end());

  const auto& perf_data = op->performance_data;

  get_tables.emplace_back(SingleGetTable{query_hex_hash, get_operator_hash(op), "NULL", "NULL",
                                          get_table_op->table_name(), get_table_op->pruned_chunk_ids().size(),
                                          get_table_op->pruned_column_ids().size(), perf_data->output_chunk_count,
                                          perf_data->output_row_count,
                                          static_cast<size_t>(perf_data->walltime.count()), description});

  _get_tables.instances.insert(_get_tables.instances.end(), get_tables.begin(), get_tables.end());
}

std::string PlanCacheCsvExporter::_process_general_operator(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash) {
  const auto& perf_data = op->performance_data;
  const auto& left_input_perf_data = op->left_input()->performance_data;

  // TODO(Martin): Do we need the table name?
  const auto type_name = std::string{magic_enum::enum_name(op->type())};
  std::stringstream ss;
  ss << camel_to_csv_row_title(type_name) << "|" << query_hex_hash << "|" << get_operator_hash(op) << "|" << get_operator_hash(op->left_input())
     << "|NULL|" << left_input_perf_data->output_chunk_count << "|" << left_input_perf_data->output_chunk_count
     << "|" << perf_data->output_chunk_count << "|" << perf_data->output_row_count << "|"
     << perf_data->walltime.count() << "\n";

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
  for (const auto& el : aggregate_node->node_expressions) {
    // TODO: ensure we do not traverse too deep here, isn't the loop sufficient?
    visit_expression(el, [&](const auto& expression) {
      if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto original_node = column_expression->original_node.lock();

        if (original_node->type == LQPNodeType::StoredTable) {
          ss << "AGGREGATE|" <<query_hex_hash << "|" << get_operator_hash(op) << "|"
             << get_operator_hash(op->left_input()) << "|NULL|";
          if (original_node == node->left_input()) {
            ss << "DATA|";
          } else {
            ss << "REFERENCE|";
          }

          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& table_name = stored_table_node->table_name;
          ss << table_name << "|";

          const auto original_column_id = column_expression->original_column_id;
          const auto& perf_data = op->performance_data;
          const auto& left_input_perf_data = op->left_input()->performance_data;

          const auto sm_table = _sm.get_table(table_name);
          std::string column_name = "";
          if (original_column_id != INVALID_COLUMN_ID) {
            column_name = sm_table->column_names()[original_column_id];
          } else {
            column_name = "COUNT(*)";
          }

          ss << column_name << "|";

          const auto node_expression_count = aggregate_node->node_expressions.size();
          const auto group_by_column_count = aggregate_node->aggregate_expressions_begin_idx;
          ss << group_by_column_count << "|" << (node_expression_count - group_by_column_count) << "|";

          ss << left_input_perf_data->output_chunk_count << "|"
             << left_input_perf_data->output_row_count << "|" << perf_data->output_chunk_count << "|"
             << perf_data->output_row_count << "|";
          if (const auto aggregate_hash_op = dynamic_pointer_cast<const AggregateHash>(op)) {
            const auto& operator_perf_data = dynamic_cast<const OperatorPerformanceData<AggregateHash::OperatorSteps>&>(*aggregate_hash_op->performance_data);
            for (const auto step_name : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
              ss << operator_perf_data.get_step_runtime(step_name).count() << "|";
            }
          } else {
            for (auto index = size_t{0}; index < magic_enum::enum_count<AggregateHash::OperatorSteps>(); ++index) {
              ss << "NULL|";
            }
          }
          ss << perf_data->walltime.count() << "|\"";
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
          const auto& left_input_perf_data = op->left_input()->performance_data;
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

          projections.emplace_back(SingleProjection{query_hex_hash, get_operator_hash(op),
            get_operator_hash(op->left_input()), "NULL", column_type, table_name,
            column_name, left_input_perf_data->output_chunk_count, left_input_perf_data->output_row_count,
            perf_data->output_chunk_count, perf_data->output_row_count,
            static_cast<size_t>(perf_data->walltime.count()), description});
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
      //   stringstreams[visit_write_id] += query_hex_hash + "|" + proj_hex_hash_str;
      //   stringstreams[visit_write_id] += ",CALC_PROJECTION|";
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
  while (op->left_input()) {
    op = op->left_input();
  }

  Assert(op->type() == OperatorType::GetTable, "not a GetTable");
  return op;
}

void PlanCacheCsvExporter::_process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
                                        std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes) {
  std::ofstream joins_csv;
  std::ofstream general_operators_csv;
  std::ofstream aggregates_csv;

  joins_csv.open(_export_folder_name + "/joins.csv", std::ios_base::app);
  general_operators_csv.open(_export_folder_name + "/general_operators.csv", std::ios_base::app);
  aggregates_csv.open(_export_folder_name + "/aggregates.csv", std::ios_base::app);

  // TODO(anyone): handle diamonds?
  // Todo: handle index scans
  if (op->type() == OperatorType::TableScan) {
    _process_table_scan(op, query_hex_hash);
  } else if (op->type() == OperatorType::GetTable) {
    _process_get_table(op, query_hex_hash);
  } else if (op->type() == OperatorType::JoinHash || op->type() == OperatorType::JoinNestedLoop || op->type() == OperatorType::JoinSortMerge) {
    joins_csv << _process_join(op, query_hex_hash);
  } else if (op->type() == OperatorType::Aggregate) {
    aggregates_csv << _process_aggregate(op, query_hex_hash);
  } else if (op->type() == OperatorType::Projection) {
    _process_projection(op, query_hex_hash);
  } else if (op->type() == OperatorType::CreateView || op->type() == OperatorType::DropView) {
    // Both make problems when hashing their descriptions.
  } else {
    general_operators_csv << _process_general_operator(op, query_hex_hash);
  }

  visited_pqp_nodes.insert(op);

  const auto left_input = op->left_input();
  const auto right_input = op->right_input();
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
