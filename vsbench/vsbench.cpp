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
/**
 * \brief VSFS Benchmark.
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <mpi.h>
#include <time.h>
#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include "vobla/status.h"
#include "vobla/thread_pool.h"
#include "vobla/timer.h"
#include "vobla/traits.h"
#include "vsbench/driver.h"
#include "vsbench/util.h"
#include "vsfs/common/complex_query.h"
#include "vsfs/index/index_info.h"

DEFINE_string(driver, "", "Sets the test driver "
             "(hbase/mysql/voltdb/mongodb/vsfs)");
DEFINE_string(indices, "", "Sets the indices to operate on, e.g., "
              "3,4-7,8,9.");
DEFINE_int32(num_indices, 64, "Sets the total number of indices.");
DEFINE_int64(records_per_index, 100000, "Sets the number of records in each "
             " index.");
DEFINE_string(path, "/foo/bar", "Sets the path to create indices.");
DEFINE_string(op, "", "Sets the operation to performed.");
DEFINE_bool(stdin, false, "Sets to use stdin to feed records.");
DEFINE_bool(print, false, "Sets true to print out results.");
DEFINE_bool(mpi, false, "Uses MPI barrier to sync perf tests.");
DEFINE_string(query, "", "Provide the query for search operation.");

DECLARE_uint64(batch_size);

using boost::algorithm::split;
using boost::lexical_cast;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using std::stoi;
using vobla::ThreadPool;
using vobla::Timer;
using namespace vsfs::vsbench;  // NOLINT

unsigned int seed;
int mpi_rank;
int mpi_size;

namespace vsfs {
namespace vsbench {

int get_total_clients() {
  if (FLAGS_mpi) {
    int mpi_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    return mpi_size;
  } else {
    char* env = getenv("SLURM_NTASKS");
    if (!env) {
      return -1;
    }
    return stoi(env);
  }
}

int get_rank() {
  if (FLAGS_mpi) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
  } else {
    char* env = getenv("SLURM_PROCID");
    if (!env) {
      return -1;
    }
    return stoi(env);
  }
}

/**
 * \brief Parses the index string to a vector of int.
 *
 * It parses "1,3-7,8,10" to [1, 3, 4, 5, 6, 7, 8, 10].
 */
Status parse_indices(const string &index_str, vector<int> *indices) {
  CHECK_NOTNULL(indices);
  vector<string> indices_string;
  split(indices_string, index_str, boost::is_any_of(","));

  set<int> unique_indices;;
  for (const auto& idx_str : indices_string) {
    vector<string> segment;
    split(segment, idx_str, boost::is_any_of("-"));
    if (segment.size() == 1) {
      unique_indices.insert(lexical_cast<int>(segment[0]));
    } else if (segment.size() == 2) {
      int begin = lexical_cast<int>(segment[0]);
      int end = lexical_cast<int>(segment[1]);
      for (int i = begin; i <= end; ++i) {
        unique_indices.insert(i);
      }
    } else {
      return Status(-1, "Wrong format of indices.");
    }
  }
  indices->assign(unique_indices.begin(), unique_indices.end());
  return Status::OK;
}

Status create_indices() {
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  CHECK_NOTNULL(driver.get());

  Status status;
  status = driver->init();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect to " << FLAGS_driver;
    return status;
  }
  VLOG(1) << "Connected to db...";

  int index_type = vsfs::index::IndexInfo::BTREE;
  int key_type = UINT64;
  for (int i = 0; i < FLAGS_num_indices; ++i) {
    string index_name = "index" + lexical_cast<string>(i);
    VLOG(1) << "Create index: " << index_name;
    status = driver->create_index(FLAGS_path, index_name,
                                  index_type, key_type);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to create index: ("
          << FLAGS_path << ", " << index_name << ", " << index_type
          << ", " << key_type << ")";
      return status;
    }
  }
  return Status::OK;
}

/**
 * \brief Inserts records into the index with given name.
 * \param index_name the name of index.
 */
Status insert_records(const vector<string> &index_names) {
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  CHECK_NOTNULL(driver.get());

  VLOG(1) << "Connect to insert records.";
  Status status = driver->connect();
  if (!status.ok()) {
    string error_message("Failed to connect: ");
    error_message += status.message();
    LOG(ERROR) << error_message;
    return Status(-1, error_message);
  }

  for (const auto& index_name : index_names) {
    LOG(ERROR) << "Insert record for: " << index_name;
    status = Util::insert_files(driver.get(), FLAGS_path, index_name,
                                FLAGS_records_per_index);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to insert file: " << status.message();
    }
  }
  return status;
}


/**
 * \brief Inserts records into the index with given name.
 * \param index_name the name of index.
 */
Status insert_records(const string &index_name) {
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  CHECK_NOTNULL(driver.get());

  VLOG(1) << "Connect to insert records for " << index_name;
  Status status = driver->connect();
  if (!status.ok()) {
    string error_message("Failed to connect: ");
    error_message += status.message();
    LOG(ERROR) << error_message;
    return Status(-1, error_message);
  }
  VLOG(1) << "Insert record for: " << index_name;
  status = Util::insert_files(driver.get(), FLAGS_path, index_name,
                              FLAGS_records_per_index);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to insert file: " << status.message();
  }
  return status;
}

void insert_records_in_thread_pool(const vector<int>& indices) {
  // ThreadPool thread_pool(2);
  vector<string> index_names;
  for (auto idx : indices) {
    string index_name = "index" + lexical_cast<string>(idx);
    index_names.push_back(index_name);
  }
  insert_records(index_names);

  // thread_pool.add_task(std::bind(insert_records, index_name));
  // thread_pool.stop();
}

void import() {
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  Status status = driver->connect();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect:" << status.message();
    return;
  }
  auto num_clients = get_total_clients();
  int num_files = FLAGS_records_per_index;
  vector<string> files;
  if (num_clients > 0) {
    num_files /=  num_clients;
    files.reserve(num_files);
    int my_rank = get_rank();
    LOG(ERROR) << "my rank: " << my_rank << " total: " << num_clients;
    for (int i = my_rank * num_files; i < (my_rank + 1) * num_files; i++) {
      files.emplace_back(FLAGS_path + "/file-" + lexical_cast<string>(i));
    }
    LOG(ERROR) << "INsert from " << *files.begin() << " to " << files.back();
  } else {
    files.reserve(FLAGS_records_per_index);
    for (int i = 0; i < FLAGS_records_per_index; i++) {
      files.emplace_back(FLAGS_path + "/file-" + lexical_cast<string>(i));
    }
  }
  driver->import(files);
}

/**
 * \brief Populates Table in parallal.
 */
void populate() {
  // create_indices();
  vector<int> indices;
  for (int i = 0; i < FLAGS_num_indices; i++) {
    indices.push_back(i);
  }
  LOG(INFO) << "Popularte with MPI: " << FLAGS_mpi;
  if (FLAGS_mpi) {
    int records_per_client = FLAGS_records_per_index / mpi_size;
    int start = records_per_client * mpi_rank;

    LOG(INFO) << "Create driver for populating: " << FLAGS_driver;
    unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
    CHECK_NOTNULL(driver.get());

    Status status = driver->connect();
    if (!status.ok()) {
      string error_message("Failed to connect: ");
      error_message += status.message();
      LOG(ERROR) << error_message;
      return;
    }

    for (auto idx : indices) {
      string index_name = "index" + lexical_cast<string>(idx);
      VLOG(1) << "Connect to populate records for " << index_name;
      status = Util::insert_files(driver.get(), FLAGS_path, index_name,
                                  start, records_per_client);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to populate index: " << status.message();
      }
    }
  } else {
    insert_records_in_thread_pool(indices);
  }
}

/**
 * \brief Test Inserting Performance.
 */
void test_insert() {
  if (!FLAGS_stdin) {
    vector<int> indices;
    if (!FLAGS_indices.empty()) {
      parse_indices(FLAGS_indices, &indices);
    } else {
      for (int i = 0; i < FLAGS_num_indices; ++i) {
        indices.push_back(i);
      }
    }
    Timer timer;
    if (FLAGS_mpi) {
      MPI_Barrier(MPI_COMM_WORLD);
      if (mpi_rank == 0) {
        timer.start();
      }
    }
    insert_records_in_thread_pool(indices);
    if (FLAGS_mpi) {
      MPI_Barrier(MPI_COMM_WORLD);
      if (mpi_rank == 0) {
        timer.stop();
        LOG(INFO) << "INSERT TIME: " << timer.get_in_second();
      }
    }
  } else {
    unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
    string buf;
    Driver::RecordVector records;
    while (!std::cin.eof()) {
      std::getline(std::cin, buf);
      if (buf.empty()) {
        continue;
      }

      boost::tokenizer<> token(buf);
      vector<string> tokens(token.begin(), token.end());
      if (tokens.size() != 3) {
        LOG(ERROR) << "An error has occorred when read: " << buf;
        continue;
      }
      string file_path = tokens[0];
      string index_name = tokens[1];
      uint64_t key = lexical_cast<uint64_t>(tokens[2]);
      records.emplace_back(file_path, index_name, key);
      if (records.size() >= FLAGS_batch_size) {
        driver->insert(records);
        records.clear();
      }
    }
    if (!records.empty()) {
      driver->insert(records);
    }
  }
};

void insert_record(const string &file, const string& name, const string &key) {
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  Driver::RecordVector records;
  records.emplace_back(file, name, lexical_cast<uint64_t>(key));
  driver->insert(records);
}

void test_search() {
  Status status;
  unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
  vector<string> files;

  status = driver->connect();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect " << FLAGS_driver;
    return;
  }
  if (FLAGS_query.empty()) {
    LOG(ERROR) << "Must provide a query string.";
    return;
  }
  ComplexQuery query;
  status = query.parse(FLAGS_query);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to parse complex query: " << FLAGS_query
               << " : " << status.message();
    return;
  }

  Timer timer;
  if (FLAGS_mpi) {
    MPI_Barrier(MPI_COMM_WORLD);
  }

  timer.start();
  status = driver->search(query, &files);
  timer.stop();
  LOG(INFO) << "SEARCH LATENCY: " << timer.get_in_ms();

  if (FLAGS_mpi) {
    MPI_Barrier(MPI_COMM_WORLD);
    /*
    if (mpi_rank == 0) {
      timer.stop();
      LOG(INFO) << " MPI SEARCH TIME: " << timer.get_in_ms();
    }
    */
  }

  if (!status.ok()) {
    LOG(ERROR) << "Failed to search: " << status.message();
    return;
  }
  if (FLAGS_print) {
    for (const auto& file : files) {
      printf("%s\n", file.c_str());
    }
  }
}

void test_open_search() {
  vector<string> files;

  // TODO(eddyxu): merge with test_search()
  if (FLAGS_mpi) {
    MPI_Barrier(MPI_COMM_WORLD);
  }

  vector<double> latencies;
  vector<thread> threads;

  for (int i = 0; i < FLAGS_num_indices; ++i) {
    threads.emplace_back([&](int i) {
        unique_ptr<Driver> driver(Driver::create_driver(FLAGS_driver));
        try {
          if (!driver->connect().ok()) {
            LOG(ERROR) << "Failed to connect " << FLAGS_driver;
            return;
          }
        } catch (...) {  // NOLINT
        }

        string query = string("/foo/bar/?index") + lexical_cast<string>(i)
                       + "<1000";
        ComplexQuery cq;
        CHECK(cq.parse(query).ok());
        Timer timer;

        for (int j = 0; j < 10; j++) {
          timer.start();
          try {
            driver->search(cq, &files);
          } catch (...) {  // NOLINT
          }
          timer.stop();
          LOG(INFO) << "SEARCH LATENCY: " << timer.get_in_ms();
        }
      }, i);
  }
  for (auto& thd : threads) {
    thd.join();
  }
}

}  // namespace vsbench
}  // namespace vsfs

int main(int argc, char* argv[]) {
  google::SetUsageMessage("Usage: ./vsbench [options] -op "
                          "{create_indices|populate|insert}");
  google::ParseCommandLineFlags(&argc, &argv, true);

  // If it runs in MPI environment.
  if (FLAGS_mpi) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    LOG(ERROR) << "MPI INIT, my rank=" << mpi_rank;
    if (mpi_rank == 0) {
      LOG(INFO) << "Total MPI tasks: " << mpi_size;
    }
    FLAGS_indices = lexical_cast<string>(mpi_rank * FLAGS_num_indices) + "-" +
                    lexical_cast<string>((mpi_rank + 1)
                                        * FLAGS_num_indices - 1);
    LOG(INFO) << "Task " << mpi_rank << " 's indices: " << FLAGS_indices;
  } else {
    int total_clients = get_total_clients();
    if (total_clients > 0) {
      LOG(INFO) << "Running in SLURM...";
      int my_rank = get_rank();
      FLAGS_indices = lexical_cast<string>(my_rank * FLAGS_num_indices) + "-" +
          lexical_cast<string>((my_rank + 1) * FLAGS_num_indices - 1);
    }
  }

  Util::seed = time(NULL);
  seed = time(NULL);
  Status status;
  int ret = 0;
  if (FLAGS_op == "create_indices") {
    status = create_indices();
    if (!status.ok()) {
      LOG(ERROR) << "Create index: " << status.message();
      ret = status.error();
    }
  } else if (FLAGS_op == "populate") {
    populate();
  } else if (FLAGS_op == "import") {
    import();
  } else if (FLAGS_op == "insert") {
    test_insert();
  } else if (FLAGS_op == "record") {
    insert_record(argv[1], argv[2], argv[3]);
  } else if (FLAGS_op == "search") {
    test_search();
  } else if (FLAGS_op == "open_search") {
    test_open_search();
  } else {
    LOG(ERROR) << "Unsupported command: " << FLAGS_op;
    ret = 1;
  }

  if (FLAGS_mpi) {
    MPI_Finalize();
  }
  return ret;
}
