/**
 * \file util.h
 *
 * \brief Shared utility for vsbench toolset.
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#ifndef VSFS_PERF_UTIL_H_
#define VSFS_PERF_UTIL_H_

#include <string>
#include "vobla/status.h"
#include "vsfs/perf/driver.h"

using std::string;

namespace vsfs {
namespace perf {

class Util {
 public:
  // Random seed.
  static unsigned int seed;

  static Status create_index(Driver* driver, const string &index_path,
                             const string &index_name, int index_type,
                             int key_type);

  static Status insert_files(Driver* driver, const string &root_path,
                             const string &index_name, int num_files);

  static Status insert_files(Driver* driver, const string &root_path,
                             const string &index_name, int start,
                             int num_files);
};

}  // namespace perf
}  // namespace vsfs

#endif  // VSFS_PERF_UTIL_H_
