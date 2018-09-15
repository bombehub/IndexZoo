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

  virtual void run_workload() = 0;

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