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
#include "mongodb/mongodb_driver.h"

using mongo::BSONObjBuilder;
using mongo::BSONObj;
using std::vector;

DEFINE_string(mongodb_host, "localhost", "Sets the mongodb host to connect");

const char* kTestCollection = "vsfs.test";

namespace vsfs {
namespace vsbench {

Status MongoDBDriver::init() {
  auto status = connect();
  if (!status.ok()) {
    return status;
  }
  clear();
  db_conn_.ensureIndex(kTestCollection, BSON("file" << 1));
  return Status::OK;
}

Status MongoDBDriver::connect() {
  try {
    db_conn_.connect(FLAGS_mongodb_host);
  } catch (const mongo::DBException &e) {  // NOLINT
    LOG(ERROR) << "Failed to connect MongoDB " << FLAGS_mongodb_host
               << " because " << e.what();
    return Status(-1, e.what());
  }
  return Status::OK;
}

Status MongoDBDriver::create_index(const string &path, const string &name,
                                   int index_type, int key_type) {
  auto p = BSON(name << 1);
  db_conn_.ensureIndex(kTestCollection, p);
  return Status::OK;
}

Status MongoDBDriver::import(const vector<string>& files) {
  vector<BSONObj> buffer;
  for (const auto& file : files) {
    auto bson_obj = BSON("file" << file);
    buffer.push_back(bson_obj);
    // TODO(lxu): use a FLAGS to customize the buffer size.
    if (buffer.size() % 1024 == 0) {
      VLOG(0) << "Import 1024 records to MongoDB.";
      db_conn_.insert(kTestCollection, buffer);
      buffer.clear();
    }
  }
  if (!buffer.empty()) {
    db_conn_.insert(kTestCollection, buffer);
  }
  return Status::OK;
}

Status MongoDBDriver::insert(const RecordVector& records) {
  for (const auto& record : records) {
    auto query = BSON("file" << std::get<0>(record));
    auto update = BSON("$set"
        << BSON(std::get<1>(record) << static_cast<int>(std::get<2>(record))));
    // Can not batch update MongoDB?
    db_conn_.update(kTestCollection, query, update);
  }
  return Status::OK;
}

Status MongoDBDriver::search(const ComplexQuery& query,
                             vector<string>* results) {
  return Status::OK;
}

Status MongoDBDriver::clear() {
  db_conn_.dropCollection(kTestCollection);
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
