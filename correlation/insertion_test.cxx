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

#include "correlation_common.h"

void measure(const size_t secondary_index_count, const size_t init_tuple_count, const size_t insert_tuple_count, const IndexPointerType index_pointer_type, const bool measure_memory_only) {
  GenericDataTable *data_table = new GenericDataTable(sizeof(uint64_t), sizeof(uint64_t) * (secondary_index_count + 1));
  
  BTreeIndex *primary_index = new BTreeIndex();
  
  BTreeIndex **secondary_index = new BTreeIndex*[secondary_index_count];
  for (size_t i = 0; i < secondary_index_count; ++i) {
    secondary_index[i] = new BTreeIndex();
  }

  double start_memory = get_memory_mb();

  FastRandom rand_gen(0);

  GenericKey tuple_key(sizeof(uint64_t));
  GenericKey tuple_value(sizeof(uint64_t) * (secondary_index_count + 1));

  uint64_t *attr_secondary = new uint64_t[secondary_index_count];

  for (size_t i = 0; i < init_tuple_count; ++i) {
      
    // generate primary key column
    uint64_t attr_primary = rand_gen.next<uint64_t>();
    memcpy(tuple_key.raw(), (char*)(&attr_primary), sizeof(uint64_t));

    // generate secondary key columns
    for (size_t k = 0; k < secondary_index_count; ++k) {
      attr_secondary[k] = rand_gen.next<uint64_t>();
    }
    memcpy(tuple_value.raw(), (char*)(attr_secondary), sizeof(uint64_t) * secondary_index_count);

    // generate unindexed column
    uint64_t attr_last = rand_gen.next<uint64_t>();
    memcpy(tuple_value.raw() + sizeof(uint64_t) * secondary_index_count, (char*)(&attr_last), sizeof(uint64_t));

    // insert into table
    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    // update primary index
    primary_index->insert(attr_primary, offset.raw_data());

    if (index_pointer_type == LogicalPointerType) {
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], attr_primary); 
      }
    } else {
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], offset.raw_data()); 
      }
    }
  }


  double end_memory = get_memory_mb();

  std::cout << "memory: " << (end_memory - start_memory) << " MB" << std::endl;

  if (measure_memory_only == true) {
    return;
  }

  TimeMeasurer timer;

  timer.tic();

  for (size_t i = 0; i < insert_tuple_count; ++i) {
      
    // generate primary key column
    uint64_t attr_primary = rand_gen.next<uint64_t>();
    memcpy(tuple_key.raw(), (char*)(&attr_primary), sizeof(uint64_t));

    // generate secondary key columns
    for (size_t k = 0; k < secondary_index_count; ++k) {
      attr_secondary[k] = rand_gen.next<uint64_t>();
    }
    memcpy(tuple_value.raw(), (char*)(attr_secondary), sizeof(uint64_t) * secondary_index_count);

    // generate unindexed column
    uint64_t attr_last = rand_gen.next<uint64_t>();
    memcpy(tuple_value.raw() + sizeof(uint64_t) * secondary_index_count, (char*)(&attr_last), sizeof(uint64_t));

    // insert into table
    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    // update primary index
    primary_index->insert(attr_primary, offset.raw_data());

    if (index_pointer_type == LogicalPointerType) {
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], attr_primary); 
      }
    } else {
      assert(index_pointer_type == PhysicalPointerType);
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], offset.raw_data()); 
      }
    }

  }

  timer.toc();
  timer.print_us();

  std::cout << "throughput: " << insert_tuple_count * 1.0 / timer.time_us() << " M ops" << std::endl;

  delete[] attr_secondary;
  attr_secondary = nullptr;

  for (size_t i = 0; i < secondary_index_count; ++i) {
    delete secondary_index[i];
    secondary_index[i] = nullptr;
  }
  delete[] secondary_index;
  secondary_index = nullptr;

  delete primary_index;
  primary_index = nullptr;

  delete data_table;
  data_table = nullptr;
}


int main(int argc, char *argv[]) {
  if (argc != 6) {
    std::cerr << "usage: " << argv[0] << " secondary_index_count init_tuple_count insert_tuple_count pointer_type measure_memory_only" << std::endl;
    return -1;
  }

  measure(atoi(argv[1]), atoi(argv[2]), atoi(argv[3]), IndexPointerType(atoi(argv[4])), bool(atoi(argv[5])));
}