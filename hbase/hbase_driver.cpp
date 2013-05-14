/**
 * \file hbase_driver.cpp
 *
 * \brief Implementation of Hbase Driver.
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#define _BSD_SOURCE
#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <endian.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <limits>
#include <map>
#include <string>
#include <vector>
#include "vobla/traits.h"
#include "vsfs/common/complex_query.h"
#include "vsfs/common/hash_util.h"
#include "vsfs/perf/hbase/hbase/Hbase.h"
#include "vsfs/perf/hbase/hbase_driver.h"

using apache::hadoop::hbase::thrift::BatchMutation;
using apache::hadoop::hbase::thrift::ColumnDescriptor;
using apache::hadoop::hbase::thrift::Mutation;
using apache::hadoop::hbase::thrift::ScannerID;
using apache::hadoop::hbase::thrift::TRowResult;
using boost::algorithm::starts_with;
using boost::lexical_cast;
using std::map;
using std::string;
using std::vector;
namespace fs = boost::filesystem;

DEFINE_string(hbase_host, "", "Sets the hostname of hbase thrift server.");
DEFINE_int32(hbase_port, 9090, "Sets the port of hbase thrift server.");

const char kTableName[] = "vsfs";
const char kFileMapTable[] = "filemap";
const char kFilePathColumn[] = "file_path";
const char kMetaColName[] = "index_meta";

namespace vsfs {
namespace perf {

string uint64_to_big_endian_text(uint64_t value) {
  uint64_t big_value = htobe64(value);
  return string(static_cast<char*>(static_cast<void*>(&big_value)),
                sizeof(big_value));
}

uint64_t big_endian_text_to_uint64(const string &btext) {
  uint64_t big_value;
  memcpy(&big_value, btext.data(), sizeof(big_value));
  return be64toh(big_value);
}

HbaseDriver::HbaseDriver() {
}

HbaseDriver::~HbaseDriver() {
}

Status HbaseDriver::connect() {
  CHECK(!hbase_.get());
  hbase_.reset(new HbaseClient(FLAGS_hbase_host, FLAGS_hbase_port));
  hbase_->open();
  return Status::OK;
}

Status HbaseDriver::init() {
  hbase_.reset(new HbaseClient(FLAGS_hbase_host, FLAGS_hbase_port));
  hbase_->open();
  VLOG(2) << "HBase connection is established.";
  clear();
  VLOG(2) << "Cleared all HBase tables.";
  vector<ColumnDescriptor> columns;
  columns.emplace_back();
  columns.back().name = kMetaColName;
  columns.emplace_back();
  columns.back().name = "meta";

  hbase_->handler()->createTable(kTableName, columns);

  columns.clear();
  columns.emplace_back();
  columns.back().name = "file_path";
  hbase_->handler()->createTable(kFileMapTable, columns);

  return Status::OK;
}

Status HbaseDriver::create_index(const string &root, const string &name,
                                 int index_type, int key_type) {
  string table_name = get_table_name(root, name);
  string row_key = root + ":" + name;

  vector<Mutation> mutations;
  mutations.emplace_back();
  mutations.back().column = "meta:table_name";
  mutations.back().value = table_name;
  mutations.emplace_back();
  mutations.back().column = "meta:index_name";
  mutations.back().value = name;
  mutations.emplace_back();
  mutations.back().column = "meta:index_type";
  mutations.back().value = lexical_cast<string>(index_type);
  mutations.emplace_back();
  mutations.back().column = "meta:key_type";
  mutations.back().value = lexical_cast<string>(key_type);

  VLOG(2) << "Table name: " << table_name;
  map<string, string> attributes;
  hbase_->handler()->mutateRow(kTableName, row_key, mutations, attributes);

  VLOG(2) << "Create index table: " << table_name;
  vector<ColumnDescriptor> columns;
  columns.emplace_back();
  columns.back().name = "file_id";
  hbase_->handler()->createTable(table_name, columns);
  return Status::OK;
}

Status HbaseDriver::import(const vector<string> &files) {
  return Status::OK;
}

Status HbaseDriver::find_index_table(const string &dirpath,
                                     const string &index_name,
                                     string *table_name) {
  CHECK_NOTNULL(table_name);
  vector<string> columns;
  columns.push_back("meta:table_name");
  columns.push_back("meta:index_type");
  columns.push_back("meta:key_type");
  map<string, string> attributes;

  string tmp = dirpath;
  while (!tmp.empty()) {
    string prefix = tmp + ":" + index_name;
    VLOG(2) << "Looking for prefix: " << prefix;
    ScannerID scanner_id = hbase_->handler()->scannerOpenWithPrefix(
        kTableName, prefix, columns, attributes);

    while (true) {
      vector<TRowResult> results;
      hbase_->handler()->scannerGet(results, scanner_id);
      if (results.empty()) {
        VLOG(1) << "Prefix matching reaches the end.";
        break;
      }
      for (const auto& rst : results) {
        VLOG(2) << "Result for: " << rst.row << ": "
                << rst.columns.at("meta:table_name").value;
        *table_name = rst.columns.at("meta:table_name").value;
        return Status::OK;
      }
    }
    tmp = fs::path(tmp).parent_path().string();
  }
  return Status::OK;
}

Status HbaseDriver::find_sub_index_tables(const string &prefix,
                                          const string &index_name,
                                          vector<string> *tables) {
  CHECK_NOTNULL(tables);
  vector<string> columns;
  columns.push_back("meta:table_name");
  columns.push_back("meta:index_name");
  columns.push_back("meta:index_type");
  columns.push_back("meta:key_type");
  map<string, string> attributes;

  ScannerID scanner_id = hbase_->handler()->scannerOpenWithPrefix(
      kTableName, prefix, columns, attributes);
  while (true) {
    vector<TRowResult> results;
    hbase_->handler()->scannerGet(results, scanner_id);
    if (results.empty()) {
      VLOG(1) << "HBase scan reaches the end.";
      break;
    }
    for (const auto& rst : results) {
      if (rst.columns.at("meta:key_type").value ==
          lexical_cast<string>(UINT64) &&
          rst.columns.at("meta:index_name").value == index_name) {
        VLOG(1) << "Found table: " << rst.columns.at("meta:table_name").value
                << " for (" << prefix << ", " << index_name << ")";
        tables->push_back(rst.columns.at("meta:table_name").value);
      }
    }
  }
  return Status::OK;
}

Status HbaseDriver::insert(const RecordVector& records) {
  Status status;
  // if (FLAGS_prefix)
  PrefixMap prefix_map;
  reorder_records(records, &prefix_map);

  map<string, string> attributes;
  for (const auto& dir_and_records : prefix_map) {
    string dir = dir_and_records.first;
    for (const auto& index_name_and_records : dir_and_records.second) {
      string index_name = index_name_and_records.first;
      string table_name;
      status = find_index_table(dir, index_name, &table_name);
      if (!status.ok()) {
        return status;
      }
      VLOG(1) << "Found table: " << table_name << " for ("
          << dir << ", " << index_name << ")";

      vector<BatchMutation> row_batches;
      vector<BatchMutation> filemap_row_batches;
      for (const auto& record : index_name_and_records.second) {
        string file_path = std::get<0>(*record);
        uint64_t file_id = HashUtil::file_path_to_hash(file_path);
        uint64_t key = std::get<2>(*record);
        // string row(&key, sizeof(key));
        row_batches.emplace_back();
        string key_text = uint64_to_big_endian_text(key);
        string file_id_text = uint64_to_big_endian_text(file_id);
        row_batches.back().row = key_text;
        row_batches.back().mutations.emplace_back();
        row_batches.back().mutations.back().column = "file_id";
        row_batches.back().mutations.back().value = file_id_text;

        filemap_row_batches.emplace_back();
        filemap_row_batches.back().row = file_id_text;
        filemap_row_batches.back().mutations.emplace_back();
        filemap_row_batches.back().mutations.back().column = "file_path";
        filemap_row_batches.back().mutations.back().value = file_path;
      }
      VLOG(1) << "Update " << table_name;
      hbase_->handler()->mutateRows(table_name, row_batches, attributes);
      VLOG(1) << "Update " << kFileMapTable;
      hbase_->handler()->mutateRows(kFileMapTable, filemap_row_batches,
                                    attributes);
    }
  }
  return Status::OK;
}

Status HbaseDriver::search_in_index_table(
    const string &table_name, uint64_t start, uint64_t end,
    vector<string> *files) {
  CHECK_NOTNULL(files);

  Status status;
  vector<string> columns = { "file_id" };
  map<string, string> attributes;
  string start_text = uint64_to_big_endian_text(start);
  ScannerID scanner_id = hbase_->handler()->scannerOpen(
      table_name, start_text, columns, attributes);
  while (true) {
    vector<TRowResult> results;
    vector<string> file_id_texts;
    try {
      hbase_->handler()->scannerGet(results, scanner_id);
    } catch (apache::hadoop::hbase::thrift::IOError e) {
      LOG(ERROR) << "Scanner error: " << e.message;
    }
    if (results.empty()) {
      break;
    }
    for (const auto& rst : results) {
      uint64_t key = big_endian_text_to_uint64(rst.row);
      if (key > end) {
        break;
      }
      VLOG(1) << "Result: " << key;
      for (const auto& k_v : rst.columns) {
        VLOG(1) << "K: " << k_v.first << " V: "
                << big_endian_text_to_uint64(k_v.second.value);
      }
      file_id_texts.push_back(rst.columns.at("file_id:").value);
    }
    if (file_id_texts.empty()) {
      break;
    }
    VLOG(1) << "Looking for file path for " << file_id_texts.size();
    vector<string> file_path_columns = { kFilePathColumn };
    hbase_->handler()->getRowsWithColumns(
        results, kFileMapTable, file_id_texts, file_path_columns, attributes);
    for (const auto& rst : results) {
      files->push_back(rst.columns.at("file_path:").value);
    }
  }
  // hbase_->handler()->closeScanner(scanner_id);
  return Status::OK;
}

Status HbaseDriver::search(const ComplexQuery& query, vector<string> *files) {
  CHECK_NOTNULL(files);
  Status status;
  string prefix = query.root();
  vector<string> index_names;
  query.get_index_names_of_range_queries(&index_names);

  for (const auto& index_name : index_names) {
    vector<string> table_names;
    status = find_sub_index_tables(prefix, index_name, &table_names);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to search HBase: " << status.message();
      return status;
    }
    const RpcRangeQuery *range_query = query.range_query(index_name);
    for (const auto& table_name : table_names) {
      LOG(INFO) << "Low: " << range_query->lower
                << " Upper: " << range_query->upper;
      uint64_t lower = range_query->lower.empty() ? 0 :
          lexical_cast<uint64_t>(range_query->lower);
      uint64_t upper = range_query->upper.empty() ?
          std::numeric_limits<uint64_t>::max() :
          lexical_cast<uint64_t>(range_query->upper);
      status = search_in_index_table(table_name, lower, upper, files);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to search in " << table_name;
        return Status::OK;
      }
    }
  }

  return Status::OK;
}

Status HbaseDriver::clear() {
  vector<string> table_names;
  hbase_->handler()->getTableNames(table_names);
  VLOG(2) << "Get table names:";

  for (const auto& table : table_names) {
    VLOG(2) << "Deleting " << table;
    hbase_->handler()->disableTable(table);
    hbase_->handler()->deleteTable(table);
  }
  return Status::OK;
}

}  // namespace perf
}  // namespace vsfs
