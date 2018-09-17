#pragma once

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

    data_table_.reset(new GenericDataTable(key_size_, value_size_));
    primary_index_.reset(new BTreeIndex());
    secondary_index_.reset(new BTreeIndex());

    if (config_.access_type_ == BaselineIndexAccess) {
      baseline_index_.reset(new BTreeIndex());
    } else if (config_.access_type_ == CorrelationIndexAccess) {
      correlation_index_.reset(new CorrelationIndex(config_.fanout_, config_.error_bound_, config_.outlier_threshold_, config_.min_node_size_));
    }
  }

  virtual void build_table() final {

    FastRandom rand_gen;

    GenericKey tuple_key(key_size_);
    GenericKey tuple_value(value_size_);

    for (size_t tuple_id = 0; tuple_id < config_.tuple_count_; ++tuple_id) {
      
      uint64_t attr0 = rand_gen.next<uint64_t>(); // primary key
      uint64_t attr1 = 0;
      if (tuple_id < config_.tuple_count_ * 2 * 1.0 / 3) {
        attr1 = tuple_id * 2;
      } else {
        attr1 = tuple_id * 5;
      }
      uint64_t attr2 = tuple_id;
      uint64_t attr3 = rand_gen.next<uint64_t>() % 100;

      // std::cout << attr1 << " " << attr2 << std::endl;

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

    }

    if (config_.access_type_ == BaselineIndexAccess) {
      baseline_index_->construct(data_table_.get(), tuple_schema_, 2, config_.index_pointer_type_);
    } else if (config_.access_type_ == CorrelationIndexAccess) {
      correlation_index_->construct(data_table_.get(), tuple_schema_, 2, 1);
      correlation_index_->print();
    }
  }

  virtual void run_queries() final {

    uint64_t sum = 0;
    switch (config_.access_type_) {
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
    std::cout << "sum: " << sum << std::endl;
  }

  uint64_t primary_index_lookup() {

    auto &keys = primary_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;
    for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

      uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

      std::vector<uint64_t> offsets;

      primary_index_->lookup(key, offsets);

      for (auto offset : offsets) {
        char *value = data_table_->get_tuple(offset);
        size_t attr3_offset = tuple_schema_.get_attr_offset(3);
        uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

        sum += attr3_ret;
      }
    }

    return sum;

  }

  uint64_t secondary_index_lookup() {

    auto &keys = secondary_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> pkeys;
        
        secondary_index_->lookup(key, pkeys);
        
        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t attr3_offset = tuple_schema_.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> offsets;
        
        secondary_index_->lookup(key, offsets);
        
        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t attr3_offset = tuple_schema_.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    }

    return sum;

  }

  uint64_t baseline_index_lookup() {

    auto &keys = correlation_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> pkeys;
        
        baseline_index_->lookup(key, pkeys);
        
        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t attr3_offset = tuple_schema_.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> offsets;
        
        baseline_index_->lookup(key, offsets);
        
        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t attr3_offset = tuple_schema_.get_attr_offset(3);
          uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

          sum += attr3_ret;
        }
      }

    }

    return sum;

  }


  uint64_t correlation_index_lookup() {

    auto &keys = correlation_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        uint64_t lhs_host_key = 0;
        uint64_t rhs_host_key = 0;
        // find host key range
        correlation_index_->lookup(key, lhs_host_key, rhs_host_key);

        std::vector<uint64_t> pkeys;

        secondary_index_->range_lookup(lhs_host_key, rhs_host_key, pkeys);

        // std::cout << "pkeys size = " << pkeys.size() << std::endl;

        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t attr2_offset = tuple_schema_.get_attr_offset(2);
          uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);

          if (attr2_ret == key) {
            size_t attr3_offset = tuple_schema_.get_attr_offset(3);
            uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

            sum += attr3_ret;
          }
        }

      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        uint64_t lhs_host_key = 0;
        uint64_t rhs_host_key = 0;
        // find host key range
        correlation_index_->lookup(key, lhs_host_key, rhs_host_key);

        std::vector<uint64_t> offsets;

        secondary_index_->range_lookup(lhs_host_key, rhs_host_key, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t attr2_offset = tuple_schema_.get_attr_offset(2);
          uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);

          if (attr2_ret == key) {
            size_t attr3_offset = tuple_schema_.get_attr_offset(3);
            uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);

            sum += attr3_ret;
          }
        }

      }

    }

    return sum;
  }

private:

  size_t key_size_;
  size_t value_size_;

  std::vector<uint64_t> primary_keys_;
  std::vector<uint64_t> secondary_keys_;
  std::vector<uint64_t> correlation_keys_;

};