#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fast_random.h"
#include "time_measurer.h"

#include "generic_key.h"
#include "generic_data_table.h"

#include "index_all.h"

#include "btree_index.h"
#include "approx_correlation_index.h"
#include "correlation_index.h"
#include "tuple_schema.h"


uint64_t primary_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BTreeIndex *primary_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);

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

uint64_t secondary_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BTreeIndex *primary_index, BTreeIndex *secondary_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);

    std::vector<uint64_t> pkeys;
    
    secondary_index->lookup(key, pkeys);
    
    // found primary keys
    for (auto pkey : pkeys) {

      std::vector<uint64_t> offsets;

      primary_index->lookup(pkey, offsets);

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

uint64_t correlation_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BTreeIndex *primary_index, BTreeIndex *base_secondary_index, CorrelationIndex *correlation_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);

    std::vector<uint64_t> host_keys;
    // found host keys
    correlation_index->lookup(key, host_keys);

    for (auto host_key : host_keys) {
      std::vector<uint64_t> pkeys;

      base_secondary_index->lookup(host_key, pkeys);

      // std::cout << "pkeys size = " << pkeys.size() << std::endl;

      // found primary keys
      for (auto pkey : pkeys) {

        std::vector<Uint64> offsets;

        primary_index->lookup(pkey, offsets);

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
  }

  return sum;
}


uint64_t approx_correlation_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BTreeIndex *primary_index, BTreeIndex *base_secondary_index, ApproxCorrelationIndex *approx_correlation_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  uint64_t sum = 0;

  FastRandom rand_gen;
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);

    uint64_t lhs_host_key = 0;
    uint64_t rhs_host_key = 0;
    // found host key range
    approx_correlation_index->lookup(key, lhs_host_key, rhs_host_key);

    std::vector<uint64_t> pkeys;
    base_secondary_index->range_lookup(lhs_host_key, rhs_host_key, pkeys);

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

enum AccessType {
  PrimaryIndexAccess = 0,
  BaseSecondaryIndexAccess,
  SecondaryIndexAccess,
  CorrelationIndexAccess,
  ApproxCorrelationIndexAccess,
};

void test(const size_t tuple_count, const AccessType access_type, const size_t param) {

  double init_mem_size = get_memory_mb();

  TupleSchema tuple_schema;
  // add five uint64_t attributes
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));

  size_t key_size = sizeof(uint64_t);
  size_t value_size = sizeof(uint64_t) * 3;
  std::unique_ptr<GenericDataTable> data_table(
    new GenericDataTable(key_size, value_size));
  // primary index
  std::unique_ptr<BTreeIndex> primary_index(new BTreeIndex());
  std::unique_ptr<BTreeIndex> base_secondary_index(new BTreeIndex());

  std::unique_ptr<BTreeIndex> secondary_index;
  std::unique_ptr<CorrelationIndex> correlation_index;
  std::unique_ptr<ApproxCorrelationIndex> approx_correlation_index;

  if (access_type == SecondaryIndexAccess) {
    secondary_index.reset(new BTreeIndex());
  } else if (access_type == CorrelationIndexAccess) {
    correlation_index.reset(new CorrelationIndex(param));
  } else if (access_type == ApproxCorrelationIndexAccess) {
    approx_correlation_index.reset(new ApproxCorrelationIndex(param));
  }

  FastRandom rand_gen;

  GenericKey tuple_key(key_size);
  GenericKey tuple_value(value_size);

  std::vector<uint64_t> attr0s;
  std::vector<uint64_t> attr1s;
  std::vector<uint64_t> attr2s;

  for (size_t tuple_id = 0; tuple_id < tuple_count; ++tuple_id) {
    
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

    // update base secondary index
    base_secondary_index->insert(attr1, attr0);

    if (access_type == SecondaryIndexAccess) {
      // update secondary index
      secondary_index->insert(attr2, attr0);
    }

  }

  if (access_type == CorrelationIndexAccess) {
    correlation_index->construct(attr2s, attr1s);
  } else if (access_type == ApproxCorrelationIndexAccess) {
    approx_correlation_index->construct(attr2s, attr1s);
    approx_correlation_index->print();
  }

  TimeMeasurer timer;
  timer.tic();

  uint64_t sum = 0;
  switch (access_type) {
    case PrimaryIndexAccess:
    sum = primary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), attr0s);
    break;
    case BaseSecondaryIndexAccess:
    sum = secondary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), base_secondary_index.get(), attr1s);
    break;
    case SecondaryIndexAccess:
    sum = secondary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), secondary_index.get(), attr2s);
    break;
    case CorrelationIndexAccess:
    sum = correlation_index_lookup(tuple_schema, data_table.get(), primary_index.get(), base_secondary_index.get(), correlation_index.get(), attr2s);
    break;
    case ApproxCorrelationIndexAccess:
    sum = approx_correlation_index_lookup(tuple_schema, data_table.get(), primary_index.get(), base_secondary_index.get(), approx_correlation_index.get(), attr2s);
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
  if (argc != 3 && argc != 4) {
    std::cerr << "usage: " << argv[0] << " tuple_count access_type param" << std::endl;
    std::cerr << "================" << std::endl;
    std::cerr << "access_type: " << std::endl;
    std::cerr << "  [0] pindex(0) + sindex(1), lookup pindex(0)" << std::endl;
    std::cerr << "  [1] pindex(0) + sindex(1), lookup sindex(1)" << std::endl;
    std::cerr << "  [2] pindex(0) + sindex(1) + sindex(2), lookup sindex(2)" << std::endl;
    std::cerr << "  [3] pindex(0) + sindex(1) + cindex(2), lookup cindex(2)" << std::endl;
    std::cerr << "  [4] pindex(0) + sindex(1) + acindex(2), lookup acindex(2)" << std::endl;

    return -1;
  }

  size_t tuple_count = atoi(argv[1]);
  AccessType access_type = AccessType(atoi(argv[2]));
  size_t param = 2;
  if (argc == 4) {
    param = atoi(argv[3]);
  }
  test(tuple_count, access_type, param);
}
