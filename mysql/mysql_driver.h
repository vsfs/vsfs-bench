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

#ifndef MYSQL_MYSQL_DRIVER_H_
#define MYSQL_MYSQL_DRIVER_H_

#include <mysql++/mysql++.h>
#include <list>
#include <string>
#include <tuple>  // NOLINT
#include <utility>
#include <vector>
#include "vobla/status.h"
#include "vsbench/driver.h"

using std::pair;
using std::string;
using std::tuple;
using std::vector;
using vobla::Status;

namespace vsfs {
namespace vsbench {

/**
 * \brief Minic VSFS in MySQL (cluster).
 */
class MySQLDriver : public Driver {
 public:
  /// Creates database and tables with default configuration.
  static void create_database();

  MySQLDriver();

  /// Create mysql connection with customized parameters
  MySQLDriver(const string& db, const string& host,
              const string& user, const string& pass);

  virtual ~MySQLDriver();

  virtual Status connect();

  /// Creates database and tables with default configuration.
  virtual Status init();

  virtual Status create_index(const string &path, const string &name,
                      int index_type, int key_type);

  virtual Status import(const vector<string> &files);

  virtual Status insert(const RecordVector& records);

  virtual Status search(const ComplexQuery& query, vector<string> *files);

  Status clear();

  // flush all caches
  void flush();

 protected:
  /**
   * \brief Insert records in to single table.
   */
  Status insert_single_table(const RecordVector &records);

  string db_;
  string server_;
  string user_;
  string password_;
  mysqlpp::Connection conn_;
};

/**
 * \brief Each index is stored in a separated table.
 */
class PartitionedMySQLDriver : public MySQLDriver {
 public:
  PartitionedMySQLDriver();

  virtual ~PartitionedMySQLDriver();

  virtual Status init();

  virtual Status create_index(const string &path, const string &name,
                              int index_type, int key_type);
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // MYSQL_MYSQL_DRIVER_H_
