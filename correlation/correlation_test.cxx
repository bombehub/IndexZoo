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

#include "index_all.h"

#include "btree_index.h"
#include "correlation_index.h"
#include "tuple_schema.h"

#include "correlation_common.h"


std::unique_ptr<GenericDataTable> data_table;

std::unique_ptr<BTreeIndex> primary_index;
std::unique_ptr<BTreeIndex> secondary_index;

std::unique_ptr<BTreeIndex> baseline_index;
std::unique_ptr<CorrelationIndex> correlation_index;

TupleSchema tuple_schema;

size_t key_size = sizeof(uint64_t);
size_t value_size = sizeof(uint64_t) * 3;

std::vector<uint64_t> attr0s;
std::vector<uint64_t> attr1s;
std::vector<uint64_t> attr2s;

uint64_t correlation_max = 0;
uint64_t correlation_min = std::numeric_limits<uint64_t>::max();

enum AccessType {
  PrimaryIndexAccess = 0,
  SecondaryIndexAccess,
  BaselineIndexAccess,
  CorrelationIndexAccess,
};

struct Config {
  AccessType access_type_ = PrimaryIndexAccess;
  IndexPointerType index_pointer_type_ = LogicalPointerType;
  size_t tuple_count_ = 100000;
  size_t query_count_ = 100000;
  uint64_t fanout_ = 4;
  float error_bound_ = 0.1;
  float outlier_threshold_ = 0.2;
};

Config config;

void usage(FILE *out) {
  fprintf(out,
          "Command line options : index_benchmark <options> \n"
          "  -h --help                : print help message \n"
          "  -a --access              : access type \n"
          "                              -- (0) primary index lookup \n"
          "                              -- (1) secondary index lookup \n"
          "                              -- (2) baseline index lookup \n"
          "                              -- (3) correlation index lookup \n"
          "  -i --index_pointer       : index pointer type \n"
          "                              -- (0) logical pointer \n"
          "                              -- (1) physical pointer \n"
          "  -t --tuple_count         : tuple count \n"
          "  -q --query_count         : query count \n"
          "  -f --fanout              : fanout \n"
          "  -e --error_bound         : error bound \n"
          "  -o --outlier_threshold   : outlier threshold \n"
  );
}

static struct option opts[] = {
    { "access",              optional_argument, NULL, 'a' },
    { "index_pointer",       optional_argument, NULL, 'i' },
    { "tuple_count",         optional_argument, NULL, 't' },
    { "query_count",         optional_argument, NULL, 'q' },
    { "fanout",              optional_argument, NULL, 'f' },
    { "error_bound",         optional_argument, NULL, 'e' },
    { "outlier_threshold",   optional_argument, NULL, 'o' },
    { NULL, 0, NULL, 0 }
};

void parse_args(int argc, char* argv[]) {
  
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ha:i:t:q:f:e:o:", opts, &idx);

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
        config.error_bound_ = atof(optarg);
        break;
      }
      case 'o': {
        config.outlier_threshold_ = atof(optarg);
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

void init() {
  // add four uint64_t attributes
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));

  data_table.reset(new GenericDataTable(key_size, value_size));
  primary_index.reset(new BTreeIndex());
  secondary_index.reset(new BTreeIndex());

  if (config.access_type_ == BaselineIndexAccess) {
    baseline_index.reset(new BTreeIndex());
  } else if (config.access_type_ == CorrelationIndexAccess) {
    correlation_index.reset(new CorrelationIndex(config.fanout_, config.error_bound_, config.outlier_threshold_));
  } 
}

void build_table() {

  FastRandom rand_gen;

  GenericKey tuple_key(key_size);
  GenericKey tuple_value(value_size);

  for (size_t tuple_id = 0; tuple_id < config.tuple_count_; ++tuple_id) {
    
    uint64_t attr0 = rand_gen.next<uint64_t>(); // primary key
    uint64_t attr1 = tuple_id * 3;
    uint64_t attr2 = tuple_id * 2;
    uint64_t attr3 = rand_gen.next<uint64_t>() % 100;

    attr0s.push_back(attr0);
    attr1s.push_back(attr1);
    attr2s.push_back(attr2);

    memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));

    memcpy(tuple_value.raw(), (char*)(&attr1), sizeof(uint64_t));
    memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr2), sizeof(uint64_t));
    memcpy(tuple_value.raw() + sizeof(uint64_t) * 2, (char*)(&attr3), sizeof(uint64_t));

    // insert into table
    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    // update primary index
    primary_index->insert(attr0, offset.raw_data());

    // update secondary index
    if (config.index_pointer_type_ == LogicalPointerType) {
      secondary_index->insert(attr1, attr0);
    } else {
      secondary_index->insert(attr1, offset.raw_data());
    }

  }

  if (config.access_type_ == BaselineIndexAccess) {
    baseline_index->construct(data_table.get(), tuple_schema, 2, config.index_pointer_type_);
  } else if (config.access_type_ == CorrelationIndexAccess) {
    correlation_index->construct(data_table.get(), tuple_schema, 2, 1);
    correlation_index->print();
  }
}

uint64_t primary_index_lookup() {

  auto &keys = attr0s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

    std::vector<uint64_t> offsets;

    primary_index->lookup(key, offsets);

    for (auto offset : offsets) {
      char *value = data_table->get_tuple(offset);
      size_t attr3_offset = tuple_schema.get_attr_offset(3);
      uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

      sum += attr3_ret;
    }
  }

  return sum;

}

uint64_t secondary_index_lookup() {

  auto &keys = attr1s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;

  if (config.index_pointer_type_ == LogicalPointerType) {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      std::vector<uint64_t> pkeys;
      
      secondary_index->lookup(key, pkeys);
      
      std::vector<uint64_t> offsets;

      primary_index->lookup(pkeys, offsets);

      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        size_t attr3_offset = tuple_schema.get_attr_offset(3);
        uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

        sum += attr3_ret;
      }
    }

  } else {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      std::vector<uint64_t> offsets;
      
      secondary_index->lookup(key, offsets);
      
      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        size_t attr3_offset = tuple_schema.get_attr_offset(3);
        uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

        sum += attr3_ret;
      }
    }

  }

  return sum;

}

uint64_t baseline_index_lookup() {

  auto &keys = attr2s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;

  if (config.index_pointer_type_ == LogicalPointerType) {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      std::vector<uint64_t> pkeys;
      
      baseline_index->lookup(key, pkeys);
      
      std::vector<uint64_t> offsets;

      primary_index->lookup(pkeys, offsets);

      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        size_t attr3_offset = tuple_schema.get_attr_offset(3);
        uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

        sum += attr3_ret;
      }
    }

  } else {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      std::vector<uint64_t> offsets;
      
      baseline_index->lookup(key, offsets);
      
      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        size_t attr3_offset = tuple_schema.get_attr_offset(3);
        uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

        sum += attr3_ret;
      }
    }

  }

  return sum;

}


uint64_t correlation_index_lookup() {

  auto &keys = attr2s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;

  if (config.index_pointer_type_ == LogicalPointerType) {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      uint64_t lhs_host_key = 0;
      uint64_t rhs_host_key = 0;
      // find host key range
      correlation_index->lookup(key, lhs_host_key, rhs_host_key);

      std::vector<uint64_t> pkeys;

      secondary_index->range_lookup(lhs_host_key, rhs_host_key, pkeys);

      std::vector<uint64_t> offsets;

      primary_index->lookup(pkeys, offsets);

      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        
        size_t attr2_offset = tuple_schema.get_attr_offset(2);
        uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);

        if (attr2_ret == key) {
          size_t attr3_offset = tuple_schema.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    }

  } else {

    for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      uint64_t lhs_host_key = 0;
      uint64_t rhs_host_key = 0;
      // find host key range
      correlation_index->lookup(key, lhs_host_key, rhs_host_key);

      std::vector<uint64_t> offsets;

      secondary_index->range_lookup(lhs_host_key, rhs_host_key, offsets);

      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        
        size_t attr2_offset = tuple_schema.get_attr_offset(2);
        uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);

        if (attr2_ret == key) {
          size_t attr3_offset = tuple_schema.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    }

  }

  return sum;
}

void run_workload() {

  double init_mem_size = get_memory_mb();

  init();
  build_table();

  TimeMeasurer timer;
  timer.tic();

  uint64_t sum = 0;
  switch (config.access_type_) {
    case PrimaryIndexAccess:
    sum = primary_index_lookup();
    break;
    case SecondaryIndexAccess:
    sum = secondary_index_lookup();
    break;
    case BaselineIndexAccess:
    sum = baseline_index_lookup();
    break;
    case CorrelationIndexAccess:
    sum = correlation_index_lookup();
    break;
    default:
    break;
  }

  timer.toc();
  
  double total_mem_size = get_memory_mb();

  std::cout << "ops: " <<  config.query_count_ * 1.0 / timer.time_us() * 1000 << " K ops." << std::endl;
  std::cout << "memory size: " << init_mem_size << " MB, " << total_mem_size << " MB." << std::endl;
  std::cout << "sum: " << sum << std::endl;
}


int main(int argc, char *argv[]) {
  parse_args(argc, argv);
  run_workload();
}
