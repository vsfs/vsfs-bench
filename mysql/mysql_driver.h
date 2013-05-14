/**
 * \brief     Build an index in mysql
 *
 * Copyright  Lei Xu <eddyxu@gmail.com>
 */

#ifndef VSFS_PERF_MYSQL_MYSQL_DRIVER_H_
#define VSFS_PERF_MYSQL_MYSQL_DRIVER_H_

#include <mysql++/mysql++.h>
#include <list>
#include <string>
#include <tuple>  // NOLINT
#include <utility>
#include <vector>
#include "vobla/status.h"
#include "vsfs/perf/driver.h"

using std::pair;
using std::string;
using std::tuple;
using std::vector;
using vobla::Status;

namespace vsfs {
namespace perf {

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

}  // namespace perf
}  // namespace vsfs

#endif  // VSFS_PERF_MYSQL_MYSQL_DRIVER_H_
