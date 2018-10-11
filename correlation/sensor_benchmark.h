#pragma once

#include <fstream>
#include "base_benchmark.h"

class SensorBenchmark : public BaseBenchmark {

public:
  SensorBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~SensorBenchmark() {}

private:

  virtual void init() final {
    // add four uint64_t attributes
    tuple_schema_.add_attr(sizeof(uint64_t));
    for (size_t i = 0; i < 16; ++i) {
      tuple_schema_.add_attr(sizeof(uint64_t));
    }

    key_size_ = sizeof(uint64_t);
    value_size_ = sizeof(uint64_t) * 16;

    primary_column_id_ = 0;
    secondary_column_id_ = 3;
    correlation_column_id_ = 4;
    read_column_id_ = 5;
  }

  virtual void build_table() final {
    
    GenericKey tuple_key(key_size_);
    GenericKey tuple_value(value_size_);

    std::string filename = "/home/yingjun/Downloads/data/ethylene.dat";

    std::ofstream outfile("data.txt");

    if (outfile.is_open() == false) { assert(false); }

    FILE *file = fopen(filename.c_str(), "r+b");
    fseek(file, 0L, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0L, SEEK_SET);

    uint64_t time = 0;
    uint64_t sensors[16];

    uint64_t tuple_id = 0;

    size_t curr_offset = 0;
    while (curr_offset < file_size) {

      size_t ret = fread(&time, sizeof(time), 1, file);
      assert(ret == 1);
      
      ret = fread(sensors, sizeof(uint64_t) * 16, 1, file);
      assert(ret == 1);

      curr_offset += sizeof(uint64_t) * 17;

      uint64_t attr0 = time;
      uint64_t attr1 = sensors[2];
      uint64_t attr2 = sensors[3];
      uint64_t attr3 = sensors[4];

      outfile << attr0 << "," << attr1 << "," << attr2 << "," << attr3 << std::endl;

      primary_keys_.push_back(attr0);
      secondary_keys_.push_back(attr1);
      correlation_keys_.push_back(attr2);

      memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));
      memcpy(tuple_value.raw(), (char*)(sensors), sizeof(uint64_t) * 16);

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
