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

#ifndef MONGODB_MONGO_DRIVER_H_
#define MONGODB_MONGO_DRIVER_H_

#include <string>
#include <vector>

using std::string;
using std::vector;

namespace vsfs {
namespace vsbench {

class MongoDBDriver : public Driver {
 public:
  MongoDBDriver() = default;

  virtual MongoDBDriver() {}

  virtual Status connect();

  virtual Status import(const vector<string>& files);

  virtual Status insert(const RecordVector& records);

  virtual Status search(const ComplexQuery& query, vector<string>* results);

  virtual Status clear();
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // MONGODB_MONGO_DRIVER_H_
