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
          "  -d --distribution        : distribution type \n"
          "                              -- (0) linear (default) \n"
          "                              -- (1) sigmoid \n"
          "  -u --query               : query type \n"
          "                              -- (0) point (default) \n"
          "                              -- (1) range \n"
          "  -s --selectivity         : selectivity for range query \n"
          "  -r --outlier_ratio       : outlier ratio \n"
          "  -c --compute             : compute type \n"
          "                              -- (0) interpolation (default) \n"
          "                              -- (1) regression \n"
          "  -t --tuple_count         : tuple count \n"
          "  -q --query_count         : query count \n"
          "  -f --fanout              : fanout \n"
          "  -e --error_bound         : error bound \n"
          "  -o --outlier_threshold   : outlier threshold \n"
          "  -n --min_node_size       : min node size \n"
          "  -m --max_height          : max height \n"
          "  -v --verbose             : verbose \n"
  );
}

static struct option opts[] = {
    { "access",              optional_argument, NULL, 'a' },
    { "index_pointer",       optional_argument, NULL, 'i' },
    { "benchmark",           optional_argument, NULL, 'b' },
    { "distribution",        optional_argument, NULL, 'd' },
    { "query",               optional_argument, NULL, 'u' },
    { "selectivity",         optional_argument, NULL, 's' },
    { "outlier_ratio",       optional_argument, NULL, 'r' },
    { "tuple_count",         optional_argument, NULL, 't' },
    { "query_count",         optional_argument, NULL, 'q' },
    /////////////////////////
    { "compute",             optional_argument, NULL, 'c' },
    { "fanout",              optional_argument, NULL, 'f' },
    { "error_bound",         optional_argument, NULL, 'e' },
    { "outlier_threshold",   optional_argument, NULL, 'o' },
    { "min_node_size",       optional_argument, NULL, 'n' },
    { "max_height",          optional_argument, NULL, 'm' },
    /////////////////////////
    { "verbose",             optional_argument, NULL, 'v' },
    { NULL, 0, NULL, 0 }
};

void parse_args(int argc, char* argv[], Config &config) {
  
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hva:i:b:d:c:t:q:s:r:f:e:o:n:m:u:", opts, &idx);

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
      case 'd': {
        config.distribution_type_ = (DistributionType)atoi(optarg);
        break;
      }
      case 'c': {
        config.correlation_index_params_.compute_type_ = (ComputeType)atoi(optarg);
        break;
      }
      case 'u': {
        config.query_type_ = (QueryType)atoi(optarg);
        break;
      }
      case 's': {
        config.selectivity_ = atof(optarg);
        break;
      }
      case 'r': {
        config.outlier_ratio_ = atof(optarg);
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
        config.correlation_index_params_.fanout_ = atoi(optarg);
        break;
      }
      case 'e': {
        config.correlation_index_params_.error_bound_ = atof(optarg);
        break;
      }
      case 'o': {
        config.correlation_index_params_.outlier_threshold_ = atof(optarg);
        break;
      }
      case 'n': {
        config.correlation_index_params_.min_node_size_ = atoi(optarg);
        break;
      }
      case 'm': {
        config.correlation_index_params_.max_height_ = atoi(optarg);
        break;
      }
      case 'v': {
        config.verbose_ = true;
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
