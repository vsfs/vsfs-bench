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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <string>
#include <vector>
#include "mongo/client/dbclient.h"
#include "vsbench/mongodb/mongodb_driver.h"
#include "vsfs/common/complex_query.h"

using mongo::BSONObjBuilder;
using mongo::BSONObj;
using std::vector;

DEFINE_string(mongodb_host, "localhost", "Sets the mongodb host to connect");
DEFINE_int32(mongodb_port, 27018, "Sets the mongodb port to connect");

const char* kTestCollection = "vsfs.test";

namespace vsfs {
namespace vsbench {

MongoDBDriver::MongoDBDriver() {
  db_conn_.reset(new mongo::DBClientConnection);
}

MongoDBDriver::~MongoDBDriver() {
}

Status MongoDBDriver::init() {
  auto status = connect();
  if (!status.ok()) {
    return status;
  }
  clear();
  return Status::OK;
}

Status MongoDBDriver::connect() {
  try {
    db_conn_->connect(FLAGS_mongodb_host + ":"
                     + std::to_string(FLAGS_mongodb_port));
  } catch (const mongo::DBException &e) {  // NOLINT
    LOG(ERROR) << "Failed to connect MongoDB " << FLAGS_mongodb_host
               << " because " << e.what();
    return Status(-1, e.what());
  }
  return Status::OK;
}

Status MongoDBDriver::create_index(const string &path, const string &name,
                                   int index_type, int key_type) {
  return Status::OK;
}

Status MongoDBDriver::import(const vector<string>& files) {
  db_conn_->setWriteConcern(mongo::W_NORMAL);
  vector<BSONObj> buffer;
  for (const auto& file : files) {
    auto bson_obj = BSON("file" << file);
    buffer.push_back(bson_obj);
    // TODO(lxu): use a FLAGS to customize the buffer size.
    if (buffer.size() % 1024 == 0) {
      db_conn_->insert(kTestCollection, buffer);
      buffer.clear();
    }
  }
  if (!buffer.empty()) {
    db_conn_->insert(kTestCollection, buffer);
  }
  return Status::OK;
}

Status MongoDBDriver::insert(const RecordVector& records) {
  for (const auto& record : records) {
    auto query = BSON("file" << std::get<0>(record));
    auto update = BSON("$set"
        << BSON(std::get<1>(record) << static_cast<int>(std::get<2>(record))));
    // Can not batch update MongoDB?
    db_conn_->update(kTestCollection, query, update);
  }
  return Status::OK;
}

Status MongoDBDriver::search(const ComplexQuery& query,
                             vector<string>* results) {
  VLOG(1) << "Query: " << query.debug_string();
  BSONObjBuilder b;
  auto prefix = BSON("file" << BSON("$regex" << ("^" + query.root())));
  b.appendElements(prefix);
  for (const auto& index_name : query.get_names_of_range_queries()) {
    auto range = query.range_query(index_name);
    VLOG(1) << "range: " << range->lower << ":" << range->upper;
    auto bson_query = BSON(index_name << mongo::GT << std::stoi(range->lower)
                           << mongo::LT << std::stoi(range->upper));
    b.appendElements(bson_query);
  }
  auto cursor = db_conn_->query(kTestCollection, b.obj());
  while (cursor->more()) {
    auto result = cursor->next();
    VLOG(1) << "File: " << result.getStringField("file");
    results->push_back(result.getStringField("file"));
  }
  return Status::OK;
}

Status MongoDBDriver::clear() {
  db_conn_->dropCollection(kTestCollection);
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
