/*
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/filesystem.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "vobla/status.h"
#include "vsbench/mysql/mysql_driver.h"
#include "vsfs/common/complex_query.h"
#include "vsfs/common/path_util.h"

namespace fs = boost::filesystem;
using std::map;
using std::pair;
using std::unique_ptr;
using std::vector;

DEFINE_string(mysql_host, "localhost", "Sets the mysql host.");
DEFINE_string(mysql_db, "vsfs", "Sets the database.");
DEFINE_string(mysql_user, "root", "Sets the user to connect MySQL.");
DEFINE_string(mysql_password, "", "Sets the password to connect MySQL.");
DEFINE_string(mysql_schema, "", "Choose one schema of mysql table to test:"
              "normal/single.");
DEFINE_bool(cal_prefix, false, "Batch the prefix of file paths.");

const char kMetaTableName[] = "index_meta";
const char kFileMetaTableName[] = "file_meta";

namespace vsfs {
namespace vsbench {

// static
void MySQLDriver::create_database() {
  Status status;
  MySQLDriver index(FLAGS_mysql_db, FLAGS_mysql_host, FLAGS_mysql_user,
                    FLAGS_mysql_password);

  status = index.init();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect MySQL: " << status.message();
  }
}

MySQLDriver::MySQLDriver() : db_(FLAGS_mysql_db), server_(FLAGS_mysql_host),
    user_(FLAGS_mysql_user), password_(FLAGS_mysql_password) {
}

MySQLDriver::MySQLDriver(const string &db, const string &server,
                       const string &user, const string &pass)
  : db_(db), server_(server), user_(user), password_(pass) {
}

MySQLDriver::~MySQLDriver() {
}

Status MySQLDriver::connect() {
  int retry = 20;
  while (retry) {
    try {
      if (conn_.connect(db_.c_str(), server_.c_str(), user_.c_str(),
                        password_.c_str())) {
        mysqlpp::MultiStatementsOption* opt =
            new mysqlpp::MultiStatementsOption(true);
        conn_.set_option(opt);
        return Status::OK;
      };
    } catch (...) {  // NOLINT
      // Sleep 0.5 second and retry connection.
      usleep(5*100000);
    }

    retry--;
  }
  string error_message("Failed to connect to MySQL: ");
  error_message += conn_.error();
  return Status(-1, error_message);
}

Status MySQLDriver::init() {
  if (!conn_.connect("", server_.c_str(), user_.c_str(), password_.c_str())) {
    return Status(-1, "Failed to connect MySQL");
  }
  clear();
  conn_.create_db(db_.c_str());
  mysqlpp::Query query = conn_.query();
  query << "use " << db_;
  query.execute();
  query << "CREATE TABLE IF NOT EXISTS " << kMetaTableName << " ("
      " path TEXT, name TEXT, "
      " index_type INT UNSIGNED, key_type INT UNSIGNED) "
      " DEFAULT CHARSET = utf8 "
      " ENGINE = NDBCLUSTER;";
  query.execute();
  query << "CREATE TABLE IF NOT EXISTS " << kFileMetaTableName <<
      " (file_id BIGINT UNSIGNED UNIQUE KEY, "
      " file_path TEXT, "
      " INDEX USING BTREE (file_id)) DEFAULT CHARSET = utf8 "
      " ENGINE = NDBCLUSTER;";
  query.execute();
  if (FLAGS_mysql_schema == "single") {
    LOG(INFO) << "Create tables for schema: single.";
    // store all metadata in a single table.
    query << "CREATE TABLE IF NOT EXISTS big_index_table_uint64 "
             " (path TEXT, name TEXT, file_key INT UNSIGNED, "
             " INDEX USING BTREE(file_key)) DEFAULT CHARSET = utf8 "
             " ENGINE = NDBCLUSTER;";
    query.execute();
  }
  return Status::OK;
}

namespace {

string get_table_name(const string &path, const string &name) {
  string table_name = path + "." + name;
  std::replace(table_name.begin(), table_name.end(), '/', '_');
  std::replace(table_name.begin(), table_name.end(), '.', '_');
  return table_name.substr(1);
}

string query_table_name(mysqlpp::Query *query, const string &file_path,
                        const string &index_name) {
  CHECK_NOTNULL(query);
  if (FLAGS_cal_prefix) {
    *query << "SELECT * from index_meta WHERE '"
        << file_path << "' LIKE concat(path, '%') AND "
        << " name = '" << index_name << "' "
        << "ORDER BY path DESC LIMIT 1;";
  } else {
    *query << "SELECT * from index_meta WHERE '"
        << file_path << "' LIKE concat(path, '/%') AND "
        << " name = '" << index_name << "' "
        << "ORDER BY path DESC LIMIT 1;";
  }
  mysqlpp::StoreQueryResult res = query->store();
  if (res) {
    string path = res[0][0].c_str();
    string name = res[0][1].c_str();
    return get_table_name(path, name);
  }
  return "";
}

}  // namespace

Status MySQLDriver::create_index(const string &path, const string &name,
                                int index_type, int key_type) {
  if (FLAGS_mysql_schema.empty() || FLAGS_mysql_schema == "normal") {
    mysqlpp::Query query = conn_.query();
    query << "INSERT INTO " << kMetaTableName
        << " (path, name, index_type, key_type) VALUE"
        << " (\"" << path << "\", \"" << name << "\", " << index_type
        << ", " << key_type << ")";
    if (!query.exec()) {
      return Status(query.errnum(), query.error());
    }

    string table_name = get_table_name(path, name);
    query << "CREATE TABLE IF NOT EXISTS " << table_name << " ("
        << "file_key INT UNSIGNED, file_id BIGINT UNSIGNED, INDEX (file_key))"
        << " ENGINE = NDBCLUSTER";
    if (!query.exec()) {
      return Status(query.errnum(), query.error());
    }
  }
  return Status::OK;
}

Status MySQLDriver::import(const vector<string> &files) {
  mysqlpp::Query query = conn_.query();
  size_t i = 0;
  const size_t kSQLInsertBatch = 128;
  for (; i < files.size(); ++i) {
    string file_path = files[i];
    uint64_t file_hash = PathUtil::path_to_hash(file_path);
    if (i % kSQLInsertBatch == 0) {
      if (i) {
        VLOG(1) << "Insert to " << kFileMetaTableName << ": " << i;
        query.execute();
      }
      query << "INSERT IGNORE INTO " << kFileMetaTableName << " VALUES ";
      query << "(" << file_hash << ", '" << file_path << "') ";
    }
    query << ", (" << file_hash << ", '" << file_path << "') ";
  }
  if (i % kSQLInsertBatch) {
    query.execute();
  }
  return Status::OK;
}

Status MySQLDriver::insert(const RecordVector& records) {
  if (FLAGS_mysql_schema.empty() || FLAGS_mysql_schema == "normal") {
    mysqlpp::Query query = conn_.query();
    // map<string, vector<pair<uint64_t, uint64_t>>> table_buffer;
    map<string, vector<pair<uint64_t, string>>> table_buffer;

    if (FLAGS_cal_prefix) {
      PrefixMap prefix_map;
      reorder_records(records, &prefix_map);
      for (const auto& prefix_and_records : prefix_map) {
        const string& parent_dir = prefix_and_records.first;
        for (const auto& index_and_records : prefix_and_records.second) {
          const string& index_name = index_and_records.first;
          string table_name = query_table_name(&query, parent_dir, index_name);
          for (const auto& record : index_and_records.second) {
            uint64_t key = std::get<2>(*record);
            table_buffer[table_name].emplace_back(key, std::get<0>(*record));
          }
        }
      }
    } else {
      for (const auto& record : records) {
        string file_path;
        string index_name;
        uint64_t key;
        std::tie(file_path, index_name, key) = record;
        string table_name = query_table_name(&query, file_path, index_name);
        table_buffer[table_name].emplace_back(key, file_path);
      }
    }

    // Inserts 128 records in a batch.
    const size_t kSQLInsertBatch = 128;
    unique_ptr<mysqlpp::Transaction> trans;
    for (const auto &table_and_records : table_buffer) {
      const string& table_name = table_and_records.first;
      size_t i = 0;
      for (; i < table_and_records.second.size(); i++) {
        const auto& file_and_key = table_and_records.second[i];

        const string &file_path = file_and_key.second;
        auto file_hash = PathUtil::path_to_hash(file_path);
        if (i % kSQLInsertBatch == 0) {
          if (i) {
            VLOG(2) << "Execute insert to " << table_name << ": " << i;
            trans->commit();
          }
          VLOG(1) << " new transaction. ";
          trans.reset(new mysqlpp::Transaction(conn_));
        }
        query << "INSERT INTO " << table_name << " VALUES ("
            << file_and_key.first << ", " << file_hash << ");";
        // query.execute();
      }
      if (i % kSQLInsertBatch) {
        trans->commit();
      }
    }
  } else if (FLAGS_mysql_schema == "single") {
    return insert_single_table(records);
  }
  return Status::OK;
}

Status MySQLDriver::insert_single_table(const RecordVector &records) {
  mysqlpp::Query query = conn_.query();
  const size_t kSQLInsertBatch = 128;
  size_t i = 0;
  for (const auto& record : records) {
    string file_path;
    string name;
    uint64_t key;
    std::tie(file_path, name, key) = record;
    if (i % kSQLInsertBatch == 0) {
      if (i) {
        query.execute();
      }
      query << "INSERT INTO big_index_table_uint64 VALUES "
           << "('" << file_path << "', '" << name << "', " << key << ")";
    } else {
      query << ", ('" << file_path << "', '" << name << "', " << key << ")";
    }
    i++;
  }
  if (i % kSQLInsertBatch) {
    query.execute();
  }
  CHECK_EQ(i, records.size());
  return Status::OK;
}

Status MySQLDriver::search(const ComplexQuery& cq, vector<string> *files) {
  CHECK_NOTNULL(files);
  Status status;
  string prefix = cq.root();
  mysqlpp::Query query = conn_.query();

  auto index_names = cq.get_names_of_range_queries();

  if (FLAGS_mysql_schema == "single") {
    query << "SELECT path FROM big_index_table_uint64 WHERE ";
    int i = 0;
    for (const auto& index_name : index_names) {
      if (i) {
        query << " OR ";
      }
      const auto range_query  = cq.range_query(index_name);
      query << " ( name = '" << index_name << "' ";
      if (!range_query->lower.empty()) {
        query << " AND file_key >= " << range_query->lower;
      }
      if (!range_query->upper.empty()) {
        query << " AND file_key <= " << range_query->upper;
      }
      query << ") ";
      i++;
    }
    VLOG(1) << query;
    auto res = query.store();
    for (size_t i = 0; i < res.num_rows(); ++i) {
      files->push_back(res[i][0].c_str());
    }
  } else {
    auto index_names = cq.get_names_of_range_queries();

    for (const auto& index_name : index_names) {
      vector<string> table_names;
      query << "SELECT * from index_meta WHERE '"
            << prefix << "' LIKE concat(path, '%') AND "
            << " name = '" << index_name << "' ";
      VLOG(1) << "Query: " << query;
      mysqlpp::StoreQueryResult res = query.store();
      for (size_t i = 0; i < res.num_rows(); ++i) {
        string path = res[i][0].c_str();
        string name = res[i][1].c_str();
        table_names.push_back(get_table_name(path, name));
      }

      auto range_query = cq.range_query(index_name);
      for (const auto& table_name : table_names) {
        query << "SELECT file_meta.file_path from file_meta," << table_name
              << " WHERE ";
        if (!range_query->lower.empty()) {
          query << table_name << ".file_key >= " << range_query->lower;
        }
        if (!range_query->lower.empty() && !range_query->upper.empty()) {
          query << " AND ";
        }
        if (!range_query->upper.empty()) {
          query << table_name << ".file_key <= " << range_query->upper;
        }
        query << " AND file_meta.file_id = " << table_name << ".file_id";
        VLOG(1) << query;
        res = query.store();
        for (size_t i = 0; i < res.num_rows(); ++i) {
          files->push_back(res[i][0].c_str());
        }
      }
    }
  }
  return Status::OK;
}

Status MySQLDriver::clear() {
  mysqlpp::Query query = conn_.query();
  query << "DROP DATABASE IF EXISTS vsfs;";
  query.execute();
  return Status::OK;
}

void MySQLDriver::flush() {
  mysqlpp::Query query = conn_.query();
  query << "FLUSH TABLES";
  query.execute();
}


PartitionedMySQLDriver::PartitionedMySQLDriver() {
}

PartitionedMySQLDriver::~PartitionedMySQLDriver() {
}

Status PartitionedMySQLDriver::init() {
  if (!conn_.connect("", server_.c_str(), user_.c_str(), password_.c_str())) {
    return Status(-1, "Failed to connect MySQL");
  }
  clear();
  conn_.create_db(db_.c_str());
  mysqlpp::Query query = conn_.query();
  query << "use " << db_;
  query.execute();

  query << "CREATE TABLE IF NOT EXISTS " << kMetaTableName << " ("
      " path TEXT, name TEXT, "
      " index_type INT UNSIGNED, key_type INT UNSIGNED, "
      " DEFAULT CHARSET = utf8 "
      " ENGINE = NDBCLUSTER;";
  query.execute();
  query << "CREATE TABLE IF NOT EXISTS " << kFileMetaTableName <<
      " (file_id BIGINT UNSIGNED UNIQUE KEY, "
      " file_path TEXT, "
      " INDEX USING BTREE (file_id)) DEFAULT CHARSET = utf8 "
      " PARTITION BY HASH(file_id) "
      " ENGINE = NDBCLUSTER;";
  query.execute();
  return Status::OK;
}

Status PartitionedMySQLDriver::create_index(
    const string &path, const string &name,
    int index_type, int key_type) {
  mysqlpp::Query query = conn_.query();
  query << "INSERT INTO " << kMetaTableName
      << " (path, name, index_type, key_type) VALUE"
      << " (\"" << path << "\", \"" << name << "\", " << index_type
      << ", " << key_type << ")";
  if (!query.exec()) {
    return Status(query.errnum(), query.error());
  }

  string table_name = get_table_name(path, name);
  query << "CREATE TABLE IF NOT EXISTS " << table_name << " ("
      << "file_key INT UNSIGNED, file_id BIGINT UNSIGNED, INDEX (file_key))"
      << " ENGINE = INNODB";
  if (!query.exec()) {
    return Status(query.errnum(), query.error());
  }
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
