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

#ifndef VSBENCH_UTIL_H_
#define VSBENCH_UTIL_H_

#include <string>
#include "vobla/status.h"
#include "vsbench/driver.h"

using std::string;

namespace vsfs {
namespace vsbench {

class Util {
 public:
  // Random seed.
  static unsigned int seed;

  static Status create_index(Driver* driver, const string &index_path,
                             const string &index_name, int index_type,
                             int key_type);

  static Status insert_files(Driver* driver, const string &root_path,
                             const string &index_name, int num_files);

  static Status insert_files(Driver* driver, const string &root_path,
                             const string &index_name, int start,
                             int num_files);
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // VSBENCH_UTIL_H_
