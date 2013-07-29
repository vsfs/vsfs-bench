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

#ifndef MONGODB_MONGODB_DRIVER_H_
#define MONGODB_MONGODB_DRIVER_H_

#include <memory>
#include <string>
#include <vector>
#include "vobla/status.h"
#include "vsbench/driver.h"
#include "mongo/client/dbclient.h"

using std::string;
using std::unique_ptr;
using std::vector;
using vobla::Status;

namespace vsfs {
namespace vsbench {

class MongoDBDriver : public Driver {
 public:
  MongoDBDriver() = default;

  virtual ~MongoDBDriver() {}

  Status init();

  Status connect();

  Status create_index(const string &path, const string &name,
                      int index_type, int key_type);

  Status import(const vector<string>& files);

  Status insert(const RecordVector& records);

  Status search(const ComplexQuery& query, vector<string>* results);

  Status clear();

 private:
  mongo::DBClientConnection db_conn_;
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // MONGODB_MONGODB_DRIVER_H_
