#pragma once

#include <fstream>
#include "base_benchmark.h"

class FlightBenchmark : public BaseBenchmark {

public:
  FlightBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~FlightBenchmark() {}

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

    std::string filename = "/home/yingjun/Downloads/flights.dat";

    std::ofstream outfile("data.txt");

    if (outfile.is_open() == false) { assert(false); }

    FILE *file = fopen(filename.c_str(), "r+b");
    fseek(file, 0L, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0L, SEEK_SET);

    uint64_t elapsed_time = 0;
    uint64_t air_time = 0;
    uint64_t distance = 0;

    uint64_t tuple_id = 0;
    size_t curr_offset = 0;
    while (curr_offset < file_size) {

      size_t ret = fread(&elapsed_time, sizeof(elapsed_time), 1, file);
      assert(ret == 1);
      ret = fread(&air_time, sizeof(air_time), 1, file);
      assert(ret == 1);
      ret = fread(&distance, sizeof(distance), 1, file);
      assert(ret == 1);
      curr_offset += sizeof(uint64_t) * 3;

      if (air_time < 20) { continue; }

      uint64_t attr0 = tuple_id;
      uint64_t attr1 = distance;
      uint64_t attr2 = air_time;
      uint64_t attr3 = elapsed_time;

      outfile << attr0 << "," << attr1 << "," << attr2 << "," << attr3 << std::endl;

      primary_keys_.push_back(attr0);
      secondary_keys_.push_back(attr1);
      correlation_keys_.push_back(attr2);

      memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));
      
      memcpy(tuple_value.raw(), (char*)(&attr1), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr2), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t) * 2, (char*)(&attr3), sizeof(uint64_t));

      // insert into table
      OffsetT offset = data_table_->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

      ++tuple_id;
      if (tuple_id == config_.tuple_count_) { break; }
    }

    fclose(file);
    std::cout << "tuple count: " << tuple_id << std::endl;
  
    outfile.close();

  }

};
