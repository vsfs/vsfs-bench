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

#include <Client.h>
#include <Parameter.hpp>
#include <ParameterSet.hpp>
#include <ProcedureCallback.hpp>
#include <Row.hpp>
#include <Table.h>
#include <TableIterator.h>
#include <WireType.h>
#include <boost/algorithm/string.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <limits>
#include <string>
#include <vector>
#include "vsbench/voltdb/voltdb_driver.h"
#include "vsfs/common/path_util.h"
#include "vsfs/common/complex_query.h"

using std::string;
using std::vector;

DEFINE_string(voltdb_host, "", "Sets hostname by a comman separated string.");
DEFINE_string(voltdb_schema, "", "Choose one schema of voltdb table to test:"
              "normal/single.");

namespace vsfs {
namespace vsbench {

/* Wraps a voltdb client to make it friendly with unique_ptr. */
struct VoltDBClient {
 public:
  VoltDBClient() : client(voltdb::Client::create()) {
  }

  voltdb::Client client;
};

class EmptyCallback : public voltdb::ProcedureCallback {
 public:
  bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception) {  // NOLINT
    if (response.failure()) {
      LOG(ERROR) << "Failed to insert file: " << response.toString();
      return false;
    }
    return true;
  }
};

VoltDBDriver::VoltDBDriver() : client_(new VoltDBClient) {
}

VoltDBDriver::~VoltDBDriver() {
}

Status VoltDBDriver::connect() {
  vector<string> hosts;
  boost::split(hosts, FLAGS_voltdb_host, boost::is_any_of(","));
  if (hosts.empty()) {
    return Status(-1, "No voltdb host is provided.");
  }
  for (const auto& host : hosts) {
    client_->client.createConnection(host);
  }
  return Status::OK;
}

Status VoltDBDriver::init() {
  return Status::OK;
}

Status VoltDBDriver::create_index(const string& path, const string& name,
                                  int index_type, int key_type) {
  return Status::OK;
}

Status VoltDBDriver::import(const vector<string>& files) {
  vector<voltdb::Parameter> param_types(2);
  param_types[0] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
  param_types[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
  voltdb::Procedure procedure("FILE_META.insert", param_types);
  for (const auto& file : files) {
    voltdb::ParameterSet* params = procedure.params();
    auto hash = PathUtil::path_to_hash(file);
    params->addInt64(hash).addString(file);
    auto response = client_->client.invoke(procedure);
    if (response.failure()) {
      LOG(ERROR) << "Failed to insert file: " << response.toString();
    }
  }
  return Status::OK;
}

Status VoltDBDriver::clear() {
  return Status::OK;
}

Status VoltDBDriver::insert(const RecordVector& records) {
  // Insert into Big Single Index Table.
  vector<voltdb::Parameter> param_types(3);
  param_types[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
  param_types[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
  param_types[2] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
  voltdb::Procedure procedure("BIG_INDEX_TABLE_UINT64.insert", param_types);
  voltdb::ParameterSet* params = procedure.params();

  boost::shared_ptr<EmptyCallback> callback(new EmptyCallback);
  for (const auto& record : records) {
    // TODO(eddyxu): batch insert.
    string file_path, index_name;
    uint64_t key;
    std::tie(file_path, index_name, key) = record;
    params->addString(file_path).addString(index_name).addInt64(key);
    client_->client.invoke(procedure, callback);
  }
  while (!client_->client.drain()) {}
  return Status::OK;
}

Status VoltDBDriver::search(const ComplexQuery& query, vector<string>* files) {
  vector<voltdb::Parameter> param_types(3);
  param_types[0] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
  param_types[1] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
  param_types[2] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
  voltdb::Procedure procedure("SearchFile", param_types);
  auto index_names = query.get_names_of_range_queries();
  auto name = index_names[0];
  const auto range_query  = query.range_query(name);
  int64_t lower = std::numeric_limits<int64_t>::min();
  int64_t upper = std::numeric_limits<int64_t>::max();
  if (!range_query->lower.empty()) {
    lower = std::stol(range_query->lower);
  }
  if (!range_query->upper.empty()) {
    upper = std::stol(range_query->upper);
  }
  voltdb::ParameterSet* params = procedure.params();
  params->addString(name).addInt64(lower).addInt64(upper);
  auto response = client_->client.invoke(procedure);
  if (response.failure()) {
    LOG(ERROR) << "Failed to search files: " << response.toString();
  }
  auto count = response.results()[0].rowCount();
  files->reserve(count);
  auto iter = response.results()[0].iterator();
  for (int i = 0; i < count; i++) {
    auto row = iter.next();
    files->push_back(row.getString(0));
  }
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
