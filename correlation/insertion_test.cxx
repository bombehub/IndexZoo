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

void measure(const size_t column_count, const size_t init_tuple_count, const size_t insert_tuple_count, const IndexPointerType index_pointer_type, const bool measure_memory_only) {
  GenericDataTable *data_table = new GenericDataTable(sizeof(uint64_t), sizeof(uint64_t) * (column_count + 1));

  size_t secondary_index_count = column_count;
  
  BTreeIndex *primary_index = new BTreeIndex();
  
  BTreeIndex **secondary_index = new BTreeIndex*[secondary_index_count];
  for (size_t i = 0; i < secondary_index_count; ++i) {
    secondary_index[i] = new BTreeIndex();
  }

  double start_memory = get_memory_mb();

  FastRandom rand_gen(0);

  GenericKey tuple_key(sizeof(uint64_t));
  GenericKey tuple_value(sizeof(uint64_t) * (column_count + 1));

  uint64_t *attr_secondary = new uint64_t[column_count];

  for (size_t i = 0; i < init_tuple_count; ++i) {
      
    // generate primary key column
    uint64_t attr_primary = rand_gen.next<uint64_t>();
    memcpy(tuple_key.raw(), (char*)(&attr_primary), sizeof(uint64_t));

    // generate secondary key columns
    for (size_t k = 0; k < column_count; ++k) {
      // attr_secondary[k] = rand_gen.next<uint64_t>();
      attr_secondary[k] = i;
    }
    memcpy(tuple_value.raw(), (char*)(attr_secondary), sizeof(uint64_t) * column_count);

    // generate unindexed column
    uint64_t attr_last = rand_gen.next<uint64_t>();
    memcpy(tuple_value.raw() + sizeof(uint64_t) * column_count, (char*)(&attr_last), sizeof(uint64_t));

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

  TimeMeasurer profiling_timer;

  long long table_time = 0;
  long long primary_time = 0;
  long long secondary_time = 0;

  TimeMeasurer timer;

  timer.tic();

  for (size_t i = 0; i < insert_tuple_count; ++i) {
      
    // generate primary key column
    uint64_t attr_primary = rand_gen.next<uint64_t>();
    memcpy(tuple_key.raw(), (char*)(&attr_primary), sizeof(uint64_t));

    // generate secondary key columns
    for (size_t k = 0; k < column_count; ++k) {
      // attr_secondary[k] = rand_gen.next<uint64_t>();
      attr_secondary[k] = i + init_tuple_count;
    }
    memcpy(tuple_value.raw(), (char*)(attr_secondary), sizeof(uint64_t) * column_count);

    profiling_timer.tic();
    // generate unindexed column
    uint64_t attr_last = rand_gen.next<uint64_t>();
    memcpy(tuple_value.raw() + sizeof(uint64_t) * column_count, (char*)(&attr_last), sizeof(uint64_t));

    // insert into table
    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    profiling_timer.toc();
    table_time += profiling_timer.time_us();

    profiling_timer.tic();
    // update primary index
    primary_index->insert(attr_primary, offset.raw_data());

    profiling_timer.toc();
    primary_time += profiling_timer.time_us();

    if (index_pointer_type == LogicalPointerType) {
    profiling_timer.tic();
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], attr_primary); 
      }
    profiling_timer.toc();
    } else {
      assert(index_pointer_type == PhysicalPointerType);
    profiling_timer.tic();
      for (size_t k = 0; k < secondary_index_count; ++k) {
        secondary_index[k]->insert(attr_secondary[k], offset.raw_data()); 
      }
    profiling_timer.toc();
    }
    secondary_time += profiling_timer.time_us();

  }

  timer.toc();
  timer.print_us();

  std::cout << "throughput: " << insert_tuple_count * 1.0 / timer.time_us() << " M ops" << std::endl;
  std::cout << "table time: " << table_time << " us" << std::endl;
  std::cout << "primary time: " << primary_time << " us" << std::endl;
  std::cout << "secondary time: " << secondary_time << " us" << std::endl;

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
    std::cerr << "usage: " << argv[0] << " column_count init_tuple_count insert_tuple_count pointer_type measure_memory_only" << std::endl;
    return -1;
  }

  measure(atoi(argv[1]), atoi(argv[2]), atoi(argv[3]), IndexPointerType(atoi(argv[4])), bool(atoi(argv[5])));
}