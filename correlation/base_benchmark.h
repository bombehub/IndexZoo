#pragma once

#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fast_random.h"
#include "time_measurer.h"

#include "generic_key.h"
#include "generic_data_table.h"

#include "btree_index.h"
#include "correlation_index.h"
#include "tuple_schema.h"
#include "correlation_common.h"

class BaseBenchmark {

public:
  BaseBenchmark(const Config &config) : config_(config) {}
  virtual ~BaseBenchmark() {}

  void run_workload() {

    double init_mem_size = get_memory_mb();

    TimeMeasurer timer;

    init();

    timer.tic();
    
    build_table();

    timer.toc();

    std::cout << "table build time: " << timer.time_ms() << " ms." << std::endl;

    timer.tic();

    run_queries();

    timer.toc();
    
    double total_mem_size = get_memory_mb();

    std::cout << "ops: " <<  config_.query_count_ * 1.0 / timer.time_us() * 1000 << " K ops." << std::endl;
    std::cout << "memory size: " << init_mem_size << " MB, " << total_mem_size << " MB." << std::endl;

  }

protected:

  virtual void init() = 0;
  virtual void build_table() = 0;
  virtual void run_queries() = 0;

protected:
  Config config_;

  std::unique_ptr<GenericDataTable> data_table_;

  std::unique_ptr<BTreeIndex> primary_index_;
  std::unique_ptr<BTreeIndex> secondary_index_;

  std::unique_ptr<BTreeIndex> baseline_index_;
  std::unique_ptr<CorrelationIndex> correlation_index_;

  uint64_t correlation_max_ = 0;
  uint64_t correlation_min_ = std::numeric_limits<uint64_t>::max();

  TupleSchema tuple_schema_;

};