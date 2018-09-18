#pragma once

#include <cstdint>
#include <sstream>

enum IndexPointerType {
  LogicalPointerType = 0,
  PhysicalPointerType,
};

enum AccessType {
  PrimaryIndexAccess = 0,
  SecondaryIndexAccess,
  BaselineIndexAccess,
  CorrelationIndexAccess,
};

enum BenchmarkType {
  MicroBenchmarkType = 0,
  TaxiBenchmarkType,
  FlightBenchmarkType,
};

enum ComputeType {
  InterpolationType = 0,
  RegressionType,
};

enum QueryType {
  PointQueryType = 0,
  RangeQueryType,
};

struct Config {
  AccessType access_type_ = PrimaryIndexAccess;
  IndexPointerType index_pointer_type_ = LogicalPointerType;
  BenchmarkType benchmark_type_ = MicroBenchmarkType;
  QueryType query_type_ = PointQueryType;
  size_t tuple_count_ = 100000;
  size_t query_count_ = 100000;
  float selectivity_ = 0.1;
  
  size_t fanout_ = 4;
  size_t error_bound_ = 1;
  float outlier_threshold_ = 0.2;
  size_t min_node_size_ = 100;
  size_t max_height_ = 10;
  ComputeType compute_type_ = InterpolationType;

  bool verbose_ = false;
};

