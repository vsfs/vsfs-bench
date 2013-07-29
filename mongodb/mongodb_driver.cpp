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

DEFINE_string(mongodb_host, "localhost", "Sets the mongodb host to connect");

namespace vsfs {
namespace vsbench {

Status MongoDBDriver::init() {
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
  return Status::OK;
}

Status MongoDBDriver::import(const vector<string>& files) {
  return Status::OK;
}

Status MongoDBDriver::insert(const RecordVector& records) {
  return Status::OK;
}

Status MongoDBDriver::search(const ComplexQuery& query,
                             vector<string>* results) {
  return Status::OK;
}

Status MongoDBDriver::clear() {
  return Status::OK;
}

}  // namespace vsbench
}  // namespace vsfs
