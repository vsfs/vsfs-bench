/**
 * \file driver.cpp
 *
 * \brief Implementation of Driver
 *
 * Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
 */

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <string>
#include "driver.h"
#include "hbase/hbase_driver.h"
#include "mysql/mysql_driver.h"
#include "vsfs/vsfs_driver.h"

using std::string;
namespace fs = boost::filesystem;

namespace vsfs {
namespace perf {

Driver* Driver::create_driver(const string &name) {
  if (name == "hbase") {
    return new HbaseDriver();
  } else if (name == "mysql") {
    return new MySQLDriver();
  } else if (name == "vsfs") {
    return new VsfsDriver();
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

}  // namespace perf
}  // namespace vsfs
