#include <iostream>
#include <pqxx/pqxx>

#include "time_measurer.h"
#include "fast_random.h"
#include "correlation_index.h"
#include "correlation_common.h"

enum MethodType {
  BaselineType = 0,
  HermitType = 1,
};

struct PostgresConfig {
  size_t num_tuples_ = 100000;
  size_t num_queries_ = 1000;
  MethodType method_type_ = BaselineType;
};

PostgresConfig config;

std::vector<uint64_t> col0_entries;
std::vector<uint64_t> col1_entries;
std::vector<uint64_t> col2_entries;

void populate(pqxx::connection &conn) {

  pqxx::work txn(conn);

  txn.exec("DROP TABLE IF EXISTS mytest;");
  txn.exec("CREATE TABLE mytest(col0 BIGINT PRIMARY KEY, col1 BIGINT, col2 BIGINT, col3 BIGINT);");
  
  for (size_t i = 0; i < config.num_tuples_; ++i) {
    txn.exec("INSERT INTO mytest VALUES (" + std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i) + ");");
    col0_entries.push_back(i);
    col1_entries.push_back(i);
    col2_entries.push_back(i);
  }

  pqxx::result R = txn.exec("SELECT count(*) FROM mytest;");
  std::cout << "number of tuples: " << R[0][0] << std::endl;

  std::string column_name = "col1";
  std::string index_name = column_name + "index";
  txn.exec("CREATE INDEX " + index_name + " ON mytest(" + column_name + ");");
  txn.commit();
}

void baseline(pqxx::connection &conn) {
  TimeMeasurer timer;
  FastRandom rand;

  pqxx::work txn(conn);

  /////////////////////////////////////
  // CONSTRUCT SECONDARY INDEX
  /////////////////////////////////////
  timer.tic();

  std::string column_name = "col2";
  std::string index_name = column_name + "index";
  txn.exec("CREATE INDEX " + index_name + " ON mytest(" + column_name + ");");

  timer.toc();

  std::cout << "construct time: " << timer.time_us() << " us" << std::endl;

  /////////////////////////////////////
  // PROCESS QUERIES
  /////////////////////////////////////
  uint64_t sum = 0;
  timer.tic();

  for (size_t i = 0; i < config.num_queries_; ++i) {
    uint64_t col2_key = col2_entries.at(rand.next<uint64_t>() % col2_entries.size());
    pqxx::result R = txn.exec("SELECT col3 FROM mytest WHERE col2 = " + std::to_string(col2_key) + ";");
    sum += atoi(R[0][0].c_str());
  }

  timer.toc();
  std::cout << "elapsed time: " << timer.time_us() << " us" << std::endl;
  std::cout << "sum: " << sum << std::endl;
}

void hermit(pqxx::connection &conn) {
  TimeMeasurer timer;
  FastRandom rand;

  pqxx::work txn(conn);

  /////////////////////////////////////
  // CONSTRUCT CORRELATION INDEX
  /////////////////////////////////////
  
  CorrelationIndexParams params;
  std::unique_ptr<CorrelationIndex> correlation_index(new CorrelationIndex(params));

  timer.tic();

  correlation_index->construct(col2_entries, col1_entries, col0_entries);

  timer.toc();

  std::cout << "construct time: " << timer.time_us() << " us" << std::endl;

  /////////////////////////////////////
  // PROCESS QUERIES
  /////////////////////////////////////
  uint64_t sum = 0;

  long long trs_time = 0;
  long long index_table_time = 0;


  for (size_t i = 0; i < config.num_queries_; ++i) {
    uint64_t col2_key = col2_entries.at(rand.next<uint64_t>() % col2_entries.size());

    uint64_t lhs_host_key = 0;
    uint64_t rhs_host_key = 0;
    std::vector<uint64_t> outliers;

    timer.tic();
    // find host key range
    bool ret = correlation_index->lookup(col2_key, lhs_host_key, rhs_host_key, outliers);
    timer.toc();

    trs_time += timer.time_us();
    
    timer.tic();
    if (ret == true) {

      pqxx::result R = txn.exec("SELECT col2,col3 FROM mytest WHERE col1 BETWEEN " + std::to_string(lhs_host_key) + " AND " + std::to_string(rhs_host_key) + ";");

      for (auto row = R.begin(); row != R.end(); ++row) {

        if (atoi(row[0].c_str()) == col2_key) {
          sum += atoi(row[1].c_str());
        }
      }
    }

    if (outliers.size() != 0) {
      for (auto outlier : outliers) {
        pqxx::result R = txn.exec("SELECT col3 FROM mytest WHERE col0 = " + std::to_string(outlier) + ";");
        sum += atoi(R[0][0].c_str());
      }
    }
    
    timer.toc();

    index_table_time += timer.time_us();

  }

  std::cout << "trs time: " << trs_time << " us" << std::endl;
  std::cout << "index table time: " << index_table_time << " us" << std::endl;
  std::cout << "total time: " << trs_time + index_table_time << " us" << std::endl;
  std::cout << "sum: " << sum << std::endl;
}



int main(int argc, char *argv[]) {
  if (argc != 4) {
    std::cerr << "usage: " << argv[0] << " num_tuples num_queries method_type" << std::endl;
    return -1;
  }
  config.num_tuples_ = atoi(argv[1]);
  config.num_queries_ = atoi(argv[2]);
  config.method_type_ = MethodType(atoi(argv[3]));
  try {
    pqxx::connection conn("dbname=test user=postgres hostaddr=127.0.0.1 port=5432");
    if (conn.is_open()) {
      std::cout << "opened database successfully: " << conn.dbname() << std::endl;

      populate(conn);

      if (config.method_type_ == BaselineType) {
        baseline(conn);
      } else {
        hermit(conn);
      }

    } else {
      std::cerr << "failed open database..." << std::endl;
      exit(EXIT_FAILURE);
    }
    conn.disconnect();
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }


}