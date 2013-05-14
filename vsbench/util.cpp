/**
 * \file util.cpp
 *
 * \brief  Implementation of various utility for vsbench.
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cstdlib>
#include <string>
#include <vector>
#include "vsfs/perf/util.h"

DEFINE_uint64(batch_size, 1024, "Sets the batch size.");

using boost::lexical_cast;

namespace vsfs {
namespace perf {

unsigned int Util::seed;

Status Util::create_index(Driver* driver, const string &index_path,
                                  const string &index_name, int index_type,
                                  int key_type) {
  CHECK_NOTNULL(driver);
  Status status = driver->create_index(index_path, index_name, index_type,
                                       key_type);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create index: ("
        << index_path << ", " << index_name << ", " << index_type
        << ", " << key_type << ")";
  }
  return status;
}

Status Util::insert_files(Driver* driver, const string &root_path,
                          const string &index_name, int start,
                          int num_files) {
  CHECK_NOTNULL(driver);
  Status status;
  Driver::RecordVector records;
  string prefix = root_path + "/file-";
  for (int i = start; i < start + num_files; ++i) {
    string filename = prefix + lexical_cast<string>(i);
    // uint64_t key = rand_r(&seed) % (num_files * 10);
    uint64_t key = static_cast<uint64_t>(i);
    records.emplace_back(filename, index_name, key);

    // TODO(lxu): use a flag to set the batch size.
    if (records.size() > FLAGS_batch_size) {
      status = driver->insert(records);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to insert: " << status.message();
        return status;
      }
      records.clear();
    }
  }
  status = driver->insert(records);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to insert: " << status.message();
    return status;
  }
  return Status::OK;
}

Status Util::insert_files(Driver* driver, const string &root_path,
                          const string &index_name, int num_files) {
  return insert_files(driver, root_path, index_name, 0, num_files);
}

}  // namespace perf
}  // namespace vsfs
