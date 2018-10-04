#pragma once

#include <cmath>
#include <random>
#include "base_benchmark.h"

class MicroBenchmark : public BaseBenchmark {

public:
  MicroBenchmark(const Config &config) : BaseBenchmark(config) {}
  virtual ~MicroBenchmark() {}

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

    FastRandom rand_gen;

    GenericKey tuple_key(key_size_);
    GenericKey tuple_value(value_size_);

    std::ofstream outfile("data.txt");

    if (outfile.is_open() == false) { assert(false); }

    if (config_.distribution_type_ == LinearType) {
      // linear distribution
      for (size_t tuple_id = 0; tuple_id < config_.tuple_count_; ++tuple_id) {

        uint64_t attr2 = rand_gen.next<uint64_t>() % config_.tuple_count_;
        uint64_t attr1 = attr2;

        if (rand_gen.next_uniform() < config_.outlier_ratio_ / 2) {
          attr1 += attr1 * 1.05;
        } else if (rand_gen.next_uniform() < config_.outlier_ratio_) {
          attr1 -= attr1 * 0.95;
        }

        secondary_keys_.push_back(attr1);
        correlation_keys_.push_back(attr2);
      }
    } else {
      // sigmoid
      assert(config_.distribution_type_ == SigmoidType);

      for (size_t tuple_id = 0; tuple_id < config_.tuple_count_; ++tuple_id) {

        uint64_t attr2 = rand_gen.next<uint64_t>() % config_.tuple_count_;

        double x = attr2 * 1.0 / config_.tuple_count_ * 12 - 6;
        uint64_t attr1 = uint64_t(1.0 / (1 + exp(-x)) * 10000000);
        
        if (rand_gen.next_uniform() < config_.outlier_ratio_) {
          attr1 *= 2;
        }

        secondary_keys_.push_back(attr1);
        correlation_keys_.push_back(attr2);
      }

    }


    for (size_t tuple_id = 0; tuple_id < config_.tuple_count_; ++tuple_id) {
      
      uint64_t attr0 = rand_gen.next<uint64_t>(); // primary key

      uint64_t attr1 = secondary_keys_.at(tuple_id);

      uint64_t attr2 = correlation_keys_.at(tuple_id);

      uint64_t attr3 = rand_gen.next<uint64_t>() % 100;

      outfile << attr0 << "," << attr1 << "," << attr2 << "," << attr3 << std::endl;

      primary_keys_.push_back(attr0);

      memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));

      memcpy(tuple_value.raw(), (char*)(&attr1), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr2), sizeof(uint64_t));
      memcpy(tuple_value.raw() + sizeof(uint64_t) * 2, (char*)(&attr3), sizeof(uint64_t));

      // insert into table
      OffsetT offset = data_table_->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

      // // update primary index
      // primary_index_->insert(attr0, offset.raw_data());

      // // update secondary index
      // if (config_.index_pointer_type_ == LogicalPointerType) {
      //   secondary_index_->insert(attr1, attr0);
      // } else {
      //   secondary_index_->insert(attr1, offset.raw_data());
      // }

    }

    outfile.close();

  }

};