#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <unistd.h>
#include <getopt.h>

#include "fast_random.h"
#include "time_measurer.h"

#include "generic_key.h"
#include "generic_data_table.h"

#include "btree_index.h"
#include "correlation_index.h"
#include "tuple_schema.h"

#include "correlation_common.h"

#include "micro_benchmark.h"
#include "taxi_benchmark.h"
#include "flight_benchmark.h"


void usage(FILE *out) {
  fprintf(out,
          "Command line options : index_benchmark <options> \n"
          "  -h --help                : print help message \n"
          "  -a --access              : access type \n"
          "                              -- (0) primary index lookup (default) \n"
          "                              -- (1) secondary index lookup \n"
          "                              -- (2) baseline index lookup \n"
          "                              -- (3) correlation index lookup \n"
          "  -i --index_pointer       : index pointer type \n"
          "                              -- (0) logical pointer (default) \n"
          "                              -- (1) physical pointer \n"
          "  -b --benchmark           : benchmark type \n"
          "                              -- (0) micro benchmark (default) \n"
          "                              -- (1) taxi benchmark \n"
          "                              -- (2) flight benchmark \n"
          "                              -- (3) TBD \n"
          "  -t --tuple_count         : tuple count \n"
          "  -q --query_count         : query count \n"
          "  -f --fanout              : fanout \n"
          "  -e --error_bound         : error bound \n"
          "  -o --outlier_threshold   : outlier threshold \n"
          "  -m --min_node_size       : min node size \n"
  );
}

static struct option opts[] = {
    { "access",              optional_argument, NULL, 'a' },
    { "index_pointer",       optional_argument, NULL, 'i' },
    { "benchmark",           optional_argument, NULL, 'b' },
    { "tuple_count",         optional_argument, NULL, 't' },
    { "query_count",         optional_argument, NULL, 'q' },
    { "fanout",              optional_argument, NULL, 'f' },
    { "error_bound",         optional_argument, NULL, 'e' },
    { "outlier_threshold",   optional_argument, NULL, 'o' },
    { "min_node_size",       optional_argument, NULL, 'm' },
    { NULL, 0, NULL, 0 }
};

void parse_args(int argc, char* argv[], Config &config) {
  
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ha:i:b:t:q:f:e:o:m:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'a': {
        config.access_type_ = (AccessType)atoi(optarg);
        break;
      }
      case 'i': {
        config.index_pointer_type_ = (IndexPointerType)atoi(optarg);
        break;
      }
      case 'b': {
        config.benchmark_type_ = (BenchmarkType)atoi(optarg);
        break;
      }
      case 't': {
        config.tuple_count_ = atoi(optarg);
        break;
      }
      case 'q': {
        config.query_count_ = atoi(optarg);
        break;
      }
      case 'f': {
        config.fanout_ = atoi(optarg);
        break;
      }
      case 'e': {
        config.error_bound_ = atoi(optarg);
        break;
      }
      case 'o': {
        config.outlier_threshold_ = atof(optarg);
        break;
      }
      case 'm': {
        config.min_node_size_ = atoi(optarg);
        break;
      }
      case 'h': {
        usage(stderr);
        exit(EXIT_FAILURE);
        break;
      }
      default: {
        fprintf(stderr, "Unknown option: -%c-\n", c);
        usage(stderr);
        exit(EXIT_FAILURE);
        break;
      }
    }
  }
}




int main(int argc, char *argv[]) {

  Config config;
  parse_args(argc, argv, config);
  
  std::unique_ptr<BaseBenchmark> benchmark;

  if (config.benchmark_type_ == MicroBenchmarkType) {

    benchmark.reset(new MicroBenchmark(config));

  } else if (config.benchmark_type_ == TaxiBenchmarkType) {

    benchmark.reset(new TaxiBenchmark(config));

  } else if (config.benchmark_type_ == FlightBenchmarkType) {

    benchmark.reset(new FlightBenchmark(config));

  }

  benchmark->run_workload();
}
