/**
 * \file vsfs_driver.cpp
 *
 * \brief Vsfs Performance Test Driver.
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <algorithm>
#include <string>
#include <vector>
#include "vsfs/client/vsfs_client.h"
#include "vsfs/perf/vsfs/vsfs_driver.h"

using boost::lexical_cast;
using std::string;
using std::vector;

namespace vsfs {

using client::VSFSRpcClient;

namespace perf {

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
  return client_->create_index(root, name, index_type, key_type);
}

Status VsfsDriver::import(const vector<string> &files) {
  return client_->import(files);
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

}  // namespace perf
}  // namespace vsfs
