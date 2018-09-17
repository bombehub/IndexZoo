#pragma once

#include <fstream>
#include "base_benchmark.h"

class TaxiBenchmark : public BaseBenchmark {

public:
  TaxiBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~TaxiBenchmark() {}

private:

  virtual void init() final {
    // add four uint64_t attributes
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));

    key_size_ = sizeof(uint64_t);
    value_size_ = sizeof(uint64_t) * 3;

    data_table_.reset(new GenericDataTable(key_size_, value_size_));
    primary_index_.reset(new BTreeIndex());
    secondary_index_.reset(new BTreeIndex());

    if (config_.access_type_ == BaselineIndexAccess) {
      baseline_index_.reset(new BTreeIndex());
    } else if (config_.access_type_ == CorrelationIndexAccess) {
      correlation_index_.reset(new CorrelationIndex(config_.fanout_, config_.error_bound_, config_.outlier_threshold_, config_.min_node_size_));
    }
  }

  virtual void build_table() final {
    GenericKey tuple_key(key_size_);
    GenericKey tuple_value(value_size_);

    std::ifstream file("/home/yingjun/Downloads/yellow_tripdata_2017-01.csv");
    std::string line;
    size_t count = 0;
    while (file.good()) {
      getline(file, line);
      std::cout << line << std::endl;

      ++count;
      if (count > 5) {
        break;
      }
    }
    file.close();

  }

  virtual void run_queries() final {}


private:

  size_t key_size_;
  size_t value_size_;

};
