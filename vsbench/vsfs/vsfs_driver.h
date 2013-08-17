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

#ifndef VSFS_VSFS_DRIVER_H_
#define VSFS_VSFS_DRIVER_H_

#include <memory>
#include <string>
#include <vector>
#include "vobla/status.h"
#include "vsfs/client/vsfs_rpc_client.h"
#include "vsbench/driver.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace vsfs {
namespace vsbench {

class VsfsDriver : public Driver {
 public:
  VsfsDriver();

  virtual ~VsfsDriver();

  virtual Status connect();

  virtual Status init();

  virtual Status create_index(const string &root, const string &name,
                              int index_type, int key_type);

  virtual Status import(const vector<string> &files);

  virtual Status insert(const RecordVector& records);

  virtual Status search(const ComplexQuery& query, vector<string> *files);

  /// Deletes all tables;
  virtual Status clear();

 private:
  unique_ptr<client::VSFSRpcClient> client_;
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // VSFS_VSFS_DRIVER_H_
