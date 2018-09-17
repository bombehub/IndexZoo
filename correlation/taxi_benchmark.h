#pragma once

#include <fstream>
#include "base_benchmark.h"

class TaxiBenchmark : public BaseBenchmark {

public:
  TaxiBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~TaxiBenchmark() {}

private:

  virtual void init() final {
    // add four uint64_t attributes
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));
    tuple_schema_.add_attr(sizeof(uint64_t));

    key_size_ = sizeof(uint64_t);
    value_size_ = sizeof(uint64_t) * 3;

    primary_column_id_ = 0;
    secondary_column_id_ = 1;
    correlation_column_id_ = 2;
    read_column_id_ = 3;
  }

  virtual void build_table() final {
    
    GenericKey tuple_key(key_size_);
    GenericKey tuple_value(value_size_);

    std::string filename = "/home/yingjun/Downloads/taxi.dat";

    FILE *file = fopen(filename.c_str(), "r+b");
    fseek(file, 0L, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0L, SEEK_SET);

    uint64_t distance = 0;
    uint64_t fare = 0;
    uint64_t total = 0;

    uint64_t tuple_id = 0;
    size_t curr_offset = 0;
    while (curr_offset < file_size) {

      size_t ret = fread(&distance, sizeof(distance), 1, file);
      assert(ret == 1);
      ret = fread(&fare, sizeof(fare), 1, file);
      assert(ret == 1);
      ret = fread(&total, sizeof(total), 1, file);
      assert(ret == 1);
      curr_offset += sizeof(uint64_t) * 3;

      uint64_t attr0 = tuple_id;
      uint64_t attr1 = distance;
      uint64_t attr2 = fare;
      uint64_t attr3 = total;

      primary_keys_.push_back(attr0);
      secondary_keys_.push_back(attr1);
      correlation_keys_.push_back(attr2);

      memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));

      memcpy(tuple_value.raw(), (char*)(&attr1), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr2), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t) * 2, (char*)(&attr3), sizeof(uint64_t));

      // insert into table
      OffsetT offset = data_table_->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

      // update primary index
      primary_index_->insert(attr0, offset.raw_data());

      // update secondary index
      if (config_.index_pointer_type_ == LogicalPointerType) {
        secondary_index_->insert(attr1, attr0);
      } else {
        secondary_index_->insert(attr1, offset.raw_data());
      }

      if (attr2 > correlation_max_) {
        correlation_max_ = attr2;
      }
      if (attr2 < correlation_min_) {
        correlation_min_ = attr2;
      }


      ++tuple_id;

      if (tuple_id == config_.tuple_count_) { break; }
      // std::cout << distance << " " << fare << " " << total << std::endl;
    }

    fclose(file);
    std::cout << "tuple count = " << tuple_id << std::endl;
  
  }

};
