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
#include <Row.hpp>
#include <Table.h>
#include <TableIterator.h>
#include <WireType.h>
#include <boost/algorithm/string.hpp>
#include <boost/utility.hpp>
#include <gflags/gflags.h>
#include <string>
#include <vector>
#include "voltdb/voltdb_driver.h"
#include "vsfs/common/hash_util.h"

using std::string;
using std::vector;

DEFINE_string(voltdb_hosts, "", "Sets hostname by a comman separated string.");
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

VoltDBDriver::VoltDBDriver() : client_(new VoltDBClient) {
}

VoltDBDriver::~VoltDBDriver() {
}

Status VoltDBDriver::connect() {
  vector<string> hosts;
  boost::split(hosts, FLAGS_voltdb_hosts, boost::is_any_of(","));
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
  voltdb::Procedure procedure("file_meta.INSERT", param_types);
  for (const auto& file : files) {
    voltdb::ParameterSet* params = procedure.params();
    auto hash = HashUtil::file_path_to_hash(file);
    params->addInt64(hash).addString(file);
    client_->client.invoke(procedure);
  }
  return Status::OK;
}

Status VoltDBDriver::clear() {
  return Status::OK;
}

Status VoltDBDriver::insert(const RecordVector& records) {
  return Status::OK;
}

Status VoltDBDriver::search(const ComplexQuery& query, vector<string>* files) {
  return Status::OK;
}


}  // namespace vsbench
}  // namespace vsfs