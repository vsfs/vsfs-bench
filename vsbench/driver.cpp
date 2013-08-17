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

#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <string>
#include "hbase/hbase_driver.h"
#include "voltdb/voltdb_driver.h"
#include "vsbench/driver.h"
#include "vsbench/mongodb/mongodb_driver.h"
#include "vsbench/mysql/mysql_driver.h"
#include "vsbench/vsfs/vsfs_driver.h"

using std::string;
namespace fs = boost::filesystem;

namespace vsfs {
namespace vsbench {

Driver* Driver::create_driver(const string &name) {
  if (name == "hbase") {
    return new HbaseDriver();
  } else if (name == "mysql") {
    return new MySQLDriver();
  } else if (name == "vsfs") {
    return new VsfsDriver();
  } else if (name == "mongodb") {
    return new MongoDBDriver();
  } else if (name == "voltdb") {
    return new VoltDBDriver();
  }
  return nullptr;
}

string Driver::get_table_name(const string &path, const string &name) {
  string table_name = path + "." + name;
  std::replace(table_name.begin(), table_name.end(), '/', '_');
  std::replace(table_name.begin(), table_name.end(), '.', '_');
  return table_name.substr(1);
}

void Driver::reorder_records(const RecordVector& records,
                             PrefixMap* prefix_map) {
  CHECK_NOTNULL(prefix_map);
  for (const RecordTuple& record : records) {
    string file_path;
    string index_name;
    uint64_t key;
    std::tie(file_path, index_name, key) = record;
    string parent_dir = fs::path(file_path).parent_path().string();
    (*prefix_map)[parent_dir][index_name].push_back(&record);
  }
}

}  // namespace vsbench
}  // namespace vsfs
