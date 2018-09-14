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
  size_t tuple_count_;
  size_t query_count_;
  AccessType access_type_;
  bool pkey_pointer_;
  uint64_t param0_;
};

Config config;

void usage(FILE *out) {
  fprintf(out,
          "Command line options : index_benchmark <options> \n"
          "  -h --help              : print help message \n"
          "  -a --access_type       : access type \n"
          "                            -- (0) primary index lookup \n"
          );
}

void parser(int argc, char *argv[]) {

  if (argc != 3 && argc != 4) {
    std::cerr << "usage: " << argv[0] << " tuple_count access_type param" << std::endl;
    std::cerr << "================" << std::endl;
    std::cerr << "access_type: " << std::endl;
    std::cerr << "  [0] pindex(0) + sindex(1), lookup pindex(0)" << std::endl;
    std::cerr << "  [1] pindex(0) + sindex(1), lookup sindex(1)" << std::endl;
    std::cerr << "  [2] pindex(0) + sindex(1) + sindex(2), lookup sindex(2)" << std::endl;
    std::cerr << "  [3] pindex(0) + sindex(1) + cindex(2), lookup cindex(2)" << std::endl;

    exit(EXIT_FAILURE);
    return;
  }

  config.tuple_count_ = atoi(argv[1]);
  config.query_count_ = config.tuple_count_;
  config.access_type_ = AccessType(atoi(argv[2]));
  config.param0_ = 2;
  if (argc == 4) {
    config.param0_ = atoi(argv[3]);
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
    correlation_index.reset(new CorrelationIndex(config.param0_));
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
    secondary_index->insert(attr1, attr0);

  }

  if (config.access_type_ == BaselineIndexAccess) {
    baseline_index->construct(data_table.get(), tuple_schema, 2);
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

  return sum;

}

uint64_t baseline_index_lookup() {

  auto &keys = attr2s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;
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

  return sum;

}


uint64_t correlation_index_lookup() {

  auto &keys = attr2s;
  size_t key_count = keys.size();

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < config.query_count_; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

    uint64_t lhs_host_key = 0;
    uint64_t rhs_host_key = 0;
    // find host key range
    correlation_index->lookup(key, lhs_host_key, rhs_host_key);

    std::vector<uint64_t> pkeys;

    secondary_index->range_lookup(lhs_host_key, rhs_host_key, pkeys);

    std::vector<Uint64> offsets;

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

  return sum;
}

void test() {

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
  std::cout << "elapsed time = " << timer.time_us() << " us" << std::endl;
  std::cout << "sum = " << sum << std::endl;
  
  double total_mem_size = get_memory_mb();
  std::cout << "mem size: " << init_mem_size << " MB, " << total_mem_size << " MB." << std::endl;
}


int main(int argc, char *argv[]) {

  test();
}
