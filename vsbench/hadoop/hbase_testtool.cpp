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
#include <cstdio>
#include <string>
#include <vector>
#include "vobla/traits.h"
#include "vsfs/index/index_info.h"
#include "vsfs/common/complex_query.h"
#include "vsbench/hadoop/hbase_driver.h"
#include "vsbench/util.h"

using std::string;
using std::vector;
using vsfs::ComplexQuery;
using vsfs::index::IndexInfo;
using vsfs::vsbench::HbaseDriver;
using vsfs::vsbench::Util;

DECLARE_string(hbase_host);

void create_index(HbaseDriver *hbase, const string &root, const string &name,
                  int num_files) {
  CHECK_NOTNULL(hbase);
  hbase->create_index(root, name, IndexInfo::BTREE, UINT64);
  Util::insert_records(hbase, root, name, num_files, nullptr);
}

void test_search_tables() {
  VLOG(1) << "Test search tables.";
  HbaseDriver hbase;
  hbase.connect();
  hbase.clear();
  hbase.init();
  int num_files = 20;
  create_index(&hbase, "/abc/edf", "name", num_files);
  create_index(&hbase, "/foo", "name", num_files);
  create_index(&hbase, "/foo/bar", "name", num_files);
  create_index(&hbase, "/foo/bar", "noname", num_files);
  create_index(&hbase, "/foo/bar/abcd", "name", num_files);
  create_index(&hbase, "/zoo", "name", num_files);

  ComplexQuery query;
  query.parse("/foo/?name>10");
  vector<string> results;
  hbase.search(query, &results);

  for (const auto& rst : results) {
    printf("%s\n", rst.c_str());
  }
}

int main(int argc, char *argv[]) {
  google::SetUsageMessage("Usage: ./hbase_testtool [options]");
  google::ParseCommandLineFlags(&argc, &argv, true);
  test_search_tables();
  return 0;
}
