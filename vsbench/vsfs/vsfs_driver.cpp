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

#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <algorithm>
#include <string>
#include <vector>
#include "vsfs/client/vsfs_client.h"
#include "vsbench/vsfs/vsfs_driver.h"

using boost::lexical_cast;
using std::string;
using std::vector;

namespace vsfs {

using client::VSFSRpcClient;

namespace vsbench {

DEFINE_string(vsfs_host, "", "Sets the master server address.");
DEFINE_int32(vsfs_port, 9876, "Sets the port of master server.");

VsfsDriver::VsfsDriver() {
}

VsfsDriver::~VsfsDriver() {
}

Status VsfsDriver::connect() {
  client_.reset(new VSFSRpcClient(FLAGS_vsfs_host, FLAGS_vsfs_port));
  return client_->init();
}

Status VsfsDriver::init() {
  return connect();
}

Status VsfsDriver::create_index(const string &root, const string &name,
                                int index_type, int key_type) {
  return client_->create_index(root, name, index_type, key_type,
                               0700, 1000, 1000);
}

Status VsfsDriver::import(const vector<string> &files) {
  client_->mkdir("/", 0777, 100, 100);
  client_->mkdir("/foo", 0777, 100, 100);
  client_->mkdir("/foo/bar", 0777, 100, 100);
  ObjectId oid;
  for (const auto& path : files) {
    client_->create(path, 0600, 100, 100, &oid);
  }
  return Status::OK;
}

Status VsfsDriver::insert(const RecordVector& records) {
  typedef client::VSFSClient::IndexUpdateRequest UpdateRequest;
  vector<UpdateRequest> updates;

  for (const auto& rec : records) {
    updates.emplace_back();
    updates.back().op = UpdateRequest::INSERT;
    updates.back().file_path = std::get<0>(rec);
    updates.back().index_name = std::get<1>(rec);
    updates.back().key = lexical_cast<string>(std::get<2>(rec));
  }
  return client_->update(updates);
}

Status VsfsDriver::search(const ComplexQuery& query, vector<string> *files) {
  return client_->search(query, files);
}

Status VsfsDriver::clear() {
  LOG(ERROR) << "VsfsDriver::clear() has not implemented.";
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
