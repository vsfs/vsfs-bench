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

#ifndef HBASE_HBASE_DRIVER_H_
#define HBASE_HBASE_DRIVER_H_

#include <memory>
#include <string>
#include <vector>
#include "vobla/status.h"
#include "vsfs/rpc/rpc_client.h"
#include "vsbench/driver.h"
#include "hbase/hbase/Hbase.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace vsfs {
namespace vsbench {

/**
 * \brief Hbase Driver.
 *
 * Connect to Hbase and manipulate Hbase for VSFS operations.
 */
class HbaseDriver : public Driver {
 public:
  HbaseDriver();

  virtual ~HbaseDriver();

  /**
   * \brief Connect to Hbase.
   */
  virtual Status connect();

  /**
   * \brief Initialize Hbase tables.
   */
  virtual Status init();

  virtual Status create_index(const string &root, const string &name,
                              int index_type, int key_type);

  virtual Status import(const vector<string> &files);

  virtual Status insert(const RecordVector& records);

  virtual Status search(const ComplexQuery& query, vector<string> *files);

  /// Deletes all tables;
  virtual Status clear();

 protected:
  typedef rpc::RpcClient<apache::hadoop::hbase::thrift::HbaseClient>
      HbaseClient;

  /**
   * \brief Scans the hbase and find the prefix of the index.
   */
  Status find_index_table(const string &dirpath, const string &index_name,
                          string *table_name);

  /**
   * \brief Finds all index tables on the sub-directories that have the
   * same index name.
   * \param[in] prefix the root directory to start scanning the sub-indices.
   * \param[in] index_name
   * \param[out] tables filled with the index paths for all index tables that
   * have the same index name.
   */
  Status find_sub_index_tables(const string &prefix, const string &index_name,
                               vector<string> *tables);

  Status search_in_index_table(const string &table_name, uint64_t start,
                               uint64_t end, vector<string> *files);

  unique_ptr<HbaseClient> hbase_;
};

}  // namespace vsbench
}  // namespace vsfs

#endif  // HBASE_HBASE_DRIVER_H_
