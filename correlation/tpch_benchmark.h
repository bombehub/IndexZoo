#pragma once

#include "base_benchmark.h"

class TpchBenchmark : public BaseBenchmark {

public:
  TpchBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~TpchBenchmark() {}

private:

  virtual void init() final {}
  virtual void build_table() final {}
  virtual void run_queries() final {}

};
