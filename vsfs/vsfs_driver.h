/**
 * \file vsfs_driver.h
 *
 * \brief VSFS perf test driver
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#ifndef VSFS_PERF_VSFS_VSFS_DRIVER_H_
#define VSFS_PERF_VSFS_VSFS_DRIVER_H_

#include <memory>
#include <string>
#include <vector>
#include "vobla/status.h"
#include "vsfs/client/vsfs_rpc_client.h"
#include "vsfs/perf/driver.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace vsfs {
namespace perf {

class VsfsDriver : public Driver {
 public:
  VsfsDriver();

  virtual ~VsfsDriver();

  virtual Status connect();

  virtual Status init();

  virtual Status create_index(const string &root, const string &name,
                              int index_type, int key_type);

  virtual Status import(const vector<string> &files);

  virtual Status insert(const RecordVector& records);

  virtual Status search(const ComplexQuery& query, vector<string> *files);

  /// Deletes all tables;
  virtual Status clear();

 private:
  unique_ptr<client::VSFSRpcClient> client_;
};

}  // namespace perf
}  // namespace vsfs

#endif  // VSFS_PERF_VSFS_VSFS_DRIVER_H_
