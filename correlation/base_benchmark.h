#pragma once

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
#include "correlation_index.h"
#include "tuple_schema.h"
#include "correlation_common.h"

class BaseBenchmark {

public:
  BaseBenchmark(const Config &config) : config_(config) {}
  virtual ~BaseBenchmark() {}

  void run_workload() {

    double init_mem_size = get_memory_mb();

    TimeMeasurer timer;

    init();
    init_base();

    timer.tic();
    
    build_table();
    build_table_base();

    timer.toc();

    std::cout << "table build time: " << timer.time_ms() << " ms." << std::endl;

    timer.tic();

    run_queries();

    timer.toc();
    
    double total_mem_size = get_memory_mb();

    std::cout << "ops: " <<  config_.query_count_ * 1.0 / timer.time_us() * 1000 << " K ops." << std::endl;
    std::cout << "memory size: " << init_mem_size << " MB, " << total_mem_size << " MB." << std::endl;

  }

protected:

  virtual void init() = 0;
  virtual void build_table() = 0;

private:
  void init_base() {    
    data_table_.reset(new GenericDataTable(key_size_, value_size_));
    primary_index_.reset(new BTreeIndex());
    secondary_index_.reset(new BTreeIndex());

    if (config_.access_type_ == BaselineIndexAccess) {
      baseline_index_.reset(new BTreeIndex());
    } else if (config_.access_type_ == CorrelationIndexAccess) {
      correlation_index_.reset(new CorrelationIndex(config_.fanout_, config_.error_bound_, config_.outlier_threshold_, config_.min_node_size_, config_.max_height_, config_.compute_type_));
    }
  }

  void build_table_base() {

    if (config_.access_type_ == BaselineIndexAccess) {
      baseline_index_->construct(data_table_.get(), tuple_schema_, correlation_column_id_, config_.index_pointer_type_);
    } else if (config_.access_type_ == CorrelationIndexAccess) {
      correlation_index_->construct(data_table_.get(), tuple_schema_, correlation_column_id_, secondary_column_id_);
      correlation_index_->print(config_.verbose_);
    }

    std::sort(primary_keys_.begin(), primary_keys_.end());
    std::sort(secondary_keys_.begin(), secondary_keys_.end());
    std::sort(correlation_keys_.begin(), correlation_keys_.end());

  }

  void run_queries() {

    if (config_.query_type_ = PointQueryType) {
      // point query
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
    } else {
      // range query
      uint64_t sum = 0;
      switch (config_.access_type_) {
        case PrimaryIndexAccess:
        sum = primary_index_range_lookup();
        break;
        case SecondaryIndexAccess:
        sum = secondary_index_range_lookup();
        break;
        case BaselineIndexAccess:
        sum = baseline_index_range_lookup();
        break;
        case CorrelationIndexAccess:
        sum = correlation_index_range_lookup();
        break;
        default:
        break;
      }
    }
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
        size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
        uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

        sum += read_column_ret;
      }
    }

    return sum;

  }

  uint64_t secondary_index_lookup_base(BTreeIndex *index, std::vector<uint64_t> &keys) {

    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> pkeys;
        
        index->lookup(key, pkeys);
        
        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
          uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

          sum += read_column_ret;
        }
      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        std::vector<uint64_t> offsets;
        
        index->lookup(key, offsets);
        
        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
          uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

          sum += read_column_ret;
        }
      }

    }

    return sum;    
  }

  uint64_t secondary_index_lookup() {
    return secondary_index_lookup_base(secondary_index_.get(), secondary_keys_);
  }

  uint64_t baseline_index_lookup() {
    return secondary_index_lookup_base(baseline_index_.get(), correlation_keys_);
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
        std::vector<uint64_t> outliers;

        // find host key range
        bool ret = correlation_index_->lookup(key, lhs_host_key, rhs_host_key, outliers);

        std::vector<uint64_t> pkeys;

        if (ret == true) {
          secondary_index_->range_lookup(lhs_host_key, rhs_host_key, pkeys); 
        }
        if (outliers.size() != 0) {
          secondary_index_->lookup(outliers, pkeys);
        }

        // std::cout << "pkeys size = " << pkeys.size() << std::endl;

        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t correlation_column_offset = tuple_schema_.get_attr_offset(correlation_column_id_);
          uint64_t correlation_column_ret = *(uint64_t*)(value + correlation_column_offset);

          if (correlation_column_ret == key) {
            size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
            uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

            sum += read_column_ret;
          }
        }

      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        uint64_t lhs_host_key = 0;
        uint64_t rhs_host_key = 0;
        std::vector<uint64_t> outliers;

        // find host key range
        bool ret = correlation_index_->lookup(key, lhs_host_key, rhs_host_key, outliers);

        std::vector<uint64_t> offsets;

        if (ret == true) {
          secondary_index_->range_lookup(lhs_host_key, rhs_host_key, offsets);
        }
        if (outliers.size() != 0) {
          secondary_index_->lookup(outliers, offsets);
        }

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t correlation_column_offset = tuple_schema_.get_attr_offset(correlation_column_id_);
          uint64_t correlation_column_ret = *(uint64_t*)(value + correlation_column_offset);

          if (correlation_column_ret == key) {
            size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
            uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

            sum += read_column_ret;
          }
        }

      }

    }

    return sum;
  }



  uint64_t primary_index_range_lookup() {

    auto &keys = primary_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;
    for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

      uint64_t lhs_key = keys.at(rand_gen.next_uniform() * (1 - config_.selectivity_) * key_count);
      uint64_t rhs_key = keys.at(lhs_key + config_.selectivity_ * key_count);

      std::vector<uint64_t> offsets;

      primary_index_->range_lookup(lhs_key, rhs_key, offsets);

      for (auto offset : offsets) {
        char *value = data_table_->get_tuple(offset);
        size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
        uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

        sum += read_column_ret;
      }
    }

    return sum;

  }

  uint64_t secondary_index_range_lookup_base(BTreeIndex *index, std::vector<uint64_t> &keys) {

    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        // uint64_t key = keys.at(rand_gen.next<uint64_t>() % key_count);

        uint64_t lhs_key = keys.at(rand_gen.next_uniform() * (1 - config_.selectivity_) * key_count);
        uint64_t rhs_key = keys.at(lhs_key + config_.selectivity_ * key_count);

        std::vector<uint64_t> pkeys;
        
        index->range_lookup(lhs_key, rhs_key, pkeys);
        
        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
          uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

          sum += read_column_ret;
        }
      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t lhs_key = keys.at(rand_gen.next_uniform() * (1 - config_.selectivity_) * key_count);
        uint64_t rhs_key = keys.at(lhs_key + config_.selectivity_ * key_count);

        std::vector<uint64_t> offsets;
        
        index->range_lookup(lhs_key, rhs_key, offsets);
        
        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
          uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

          sum += read_column_ret;
        }
      }

    }

    return sum;    
  }

  uint64_t secondary_index_range_lookup() {
    return secondary_index_range_lookup_base(secondary_index_.get(), secondary_keys_);
  }

  uint64_t baseline_index_range_lookup() {
    return secondary_index_range_lookup_base(baseline_index_.get(), correlation_keys_);
  }

  uint64_t correlation_index_range_lookup() {

    auto &keys = correlation_keys_;
    size_t key_count = keys.size();

    uint64_t sum = 0;

    FastRandom rand_gen;

    if (config_.index_pointer_type_ == LogicalPointerType) {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t lhs_key = keys.at(rand_gen.next_uniform() * (1 - config_.selectivity_) * key_count);
        uint64_t rhs_key = keys.at(lhs_key + config_.selectivity_ * key_count);

        std::vector<std::pair<uint64_t, uint64_t>> host_key_ranges;
        std::vector<uint64_t> outliers;

        // find host key range
        correlation_index_->range_lookup(lhs_key, rhs_key, host_key_ranges, outliers);

        std::vector<uint64_t> pkeys;

        for (auto host_key_range : host_key_ranges) {
          secondary_index_->range_lookup(host_key_range.first, host_key_range.second, pkeys); 
        }
        if (outliers.size() != 0) {
          secondary_index_->lookup(outliers, pkeys);
        }

        // std::cout << "pkeys size = " << pkeys.size() << std::endl;

        std::vector<uint64_t> offsets;

        primary_index_->lookup(pkeys, offsets);

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t correlation_column_offset = tuple_schema_.get_attr_offset(correlation_column_id_);
          uint64_t correlation_column_ret = *(uint64_t*)(value + correlation_column_offset);

          if (correlation_column_ret >= lhs_key && correlation_column_ret <= rhs_key) {
            size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
            uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

            sum += read_column_ret;
          }
        }

      }

    } else {

      for (size_t query_id = 0; query_id < config_.query_count_; ++query_id) {

        uint64_t lhs_key = keys.at(rand_gen.next_uniform() * (1 - config_.selectivity_) * key_count);
        uint64_t rhs_key = keys.at(lhs_key + config_.selectivity_ * key_count);

        std::vector<std::pair<uint64_t, uint64_t>> host_key_ranges;
        std::vector<uint64_t> outliers;

        // find host key range
        correlation_index_->range_lookup(lhs_key, rhs_key, host_key_ranges, outliers);

        std::vector<uint64_t> offsets;

        for (auto host_key_range : host_key_ranges) {
          secondary_index_->range_lookup(host_key_range.first, host_key_range.second, offsets);
        }
        if (outliers.size() != 0) {
          secondary_index_->lookup(outliers, offsets);
        }

        for (auto offset : offsets) {
          char *value = data_table_->get_tuple(offset);
          
          size_t correlation_column_offset = tuple_schema_.get_attr_offset(correlation_column_id_);
          uint64_t correlation_column_ret = *(uint64_t*)(value + correlation_column_offset);

          if (correlation_column_ret >= lhs_key && correlation_column_ret <= rhs_key) {
            size_t read_column_offset = tuple_schema_.get_attr_offset(read_column_id_);
            uint64_t read_column_ret = *(uint64_t*)(value + read_column_offset);

            sum += read_column_ret;
          }
        }

      }

    }

    return sum;
  }

protected:
  Config config_;

  std::unique_ptr<GenericDataTable> data_table_;

  std::unique_ptr<BTreeIndex> primary_index_;
  std::unique_ptr<BTreeIndex> secondary_index_;

  std::unique_ptr<BTreeIndex> baseline_index_;
  std::unique_ptr<CorrelationIndex> correlation_index_;

  TupleSchema tuple_schema_;

  size_t primary_column_id_;
  size_t secondary_column_id_;
  size_t correlation_column_id_;
  size_t read_column_id_;

  uint64_t correlation_max_ = 0;
  uint64_t correlation_min_ = std::numeric_limits<uint64_t>::max();


  size_t key_size_;
  size_t value_size_;

  std::vector<uint64_t> primary_keys_;
  std::vector<uint64_t> secondary_keys_;
  std::vector<uint64_t> correlation_keys_;
};