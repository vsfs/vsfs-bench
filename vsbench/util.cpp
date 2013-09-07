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
 * \file util.cpp
 * \brief  Implementation of various utility for vsbench.
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cstdlib>
#include <string>
#include <vector>
#include "vobla/timer.h"
#include "vsbench/util.h"

DEFINE_uint64(batch_size, 1024, "Sets the batch size.");

using std::string;
using std::to_string;
using vobla::Timer;

namespace vsfs {
namespace vsbench {

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

Status Util::insert_files(Driver* driver, const string& root_path,
                          const string& index_name, int start,
                          int num_files) {
  CHECK_NOTNULL(driver);
  Timer timer;
  Status status;
  Driver::RecordVector records;
  string prefix = root_path + "/file-";
  for (int i = start; i < start + num_files; ++i) {
    string filename = prefix + to_string(i);
    // uint64_t key = rand_r(&seed) % (num_files * 10);
    uint64_t key = static_cast<uint64_t>(i);
    records.emplace_back(filename, index_name, key);

    // TODO(lxu): use a flag to set the batch size.
    if (records.size() > FLAGS_batch_size) {
      timer.start();
      status = driver->insert(records);
      timer.stop();
      if (!status.ok()) {
        LOG(ERROR) << "Failed to insert: " << status.message();
        return status;
      }
      // LOG(INFO) << "INSERT LATENCY: " << timer.get_in_ms();
      records.clear();
    }
  }
  timer.start();
  status = driver->insert(records);
  timer.stop();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to insert: " << status.message();
    return status;
  }
  // LOG(INFO) << "INSERT LATENCY: " << timer.get_in_ms();
  return Status::OK;
}

Status Util::insert_files(Driver* driver, const string& root_path,
                          const string& index_name, int num_files) {
  return insert_files(driver, root_path, index_name, 0, num_files);
}

}  // namespace vsbench
}  // namespace vsfs
