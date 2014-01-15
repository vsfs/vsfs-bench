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

/**
 * \file driver.h
 * \brief Perftest Test Driver.
 * It is the base class for the drivers (e.g. Hbase driver, MySQL driver).
 */

#ifndef VSBENCH_DRIVER_H_
#define VSBENCH_DRIVER_H_

#include <map>
#include <string>
#include <tuple>
#include <vector>
#include "vobla/status.h"

using std::map;
using std::string;
using std::tuple;
using std::vector;
using vobla::Status;

namespace vsfs {

class ComplexQuery;

namespace vsbench {

class Driver {
 public:
  /// tuple<file_path, index_name, key>
  typedef tuple<string, string, uint64_t> RecordTuple;

  typedef vector<RecordTuple> RecordVector;

  static Driver* create_driver(const string &name);

  virtual ~Driver() {}

  /**
   * \brief Connect to the test storage.
   */
  virtual Status connect() = 0;

  /**
   * \brief Initialize necessary data structures on the test storage.
   *
   * For example, MySQLDriver::init() creates SQL tables on MySQL.
   */
  virtual Status init() = 0;

  /**
   * \brief Creates an index on the storage.
   */
  virtual Status create_index(const string &path, const string &name,
                              int index_type, int key_type) = 0;

  /**
   * \brief Import files
   * \param files a vector of absolute paths.
   */
  virtual Status import(const vector<string>& files) = 0;

  /// Deletes all tables.
  virtual Status clear() = 0;

  virtual Status insert(const RecordVector& records) = 0;

  virtual Status search(const ComplexQuery& query, vector<string> *files) = 0;

 protected:
  string get_table_name(const string &root, const string &name);

  /// A map of { file_path: { index1: [record1, record2, ...], ...}, ...}
  typedef map<string, map<string, vector<const RecordTuple*>>> PrefixMap;

  void reorder_records(const RecordVector& records, PrefixMap* prefix_map);
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // VSBENCH_DRIVER_H_
