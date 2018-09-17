#pragma once

#include "base_benchmark.h"

class FlightBenchmark : public BaseBenchmark {

public:
  FlightBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~FlightBenchmark() {}

private:

  virtual void init() final {}
  virtual void build_table() final {}
  virtual void run_queries() final {}
};
