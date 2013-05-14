/**
 * \file driver.h
 *
 * \brief Perftest Test Driver.
 *
 * It is the base class for the drivers (e.g. Hbase driver, MySQL driver).
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#ifndef VSFS_PERF_DRIVER_H_
#define VSFS_PERF_DRIVER_H_

#include <map>
#include <string>
#include <tuple>  // NOLINT
#include <vector>
#include "vobla/status.h"

using std::map;
using std::string;
using std::tuple;
using std::vector;
using vobla::Status;

namespace vsfs {

class ComplexQuery;

namespace perf {

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
  virtual Status import(const vector<string> &files) = 0;

  /// Deletes all tables.
  virtual Status clear() = 0;

  virtual Status insert(const RecordVector& records) = 0;

  virtual Status search(const ComplexQuery &query, vector<string> *files) = 0;

 protected:
  string get_table_name(const string &root, const string &name);

  /// A map of { file_path: { index1: [record1, record2, ...], ...}, ...}
  typedef map<string, map<string, vector<const RecordTuple*>>> PrefixMap;

  void reorder_records(const RecordVector& records, PrefixMap* prefix_map);
};

}  // namespace perf
}  // namespace vsfs

#endif  // VSFS_PERF_DRIVER_H_
