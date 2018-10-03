#pragma once

#include <vector>
#include <map>
#include <unordered_map>
#include <queue>
#include <fstream>

#include "generic_key.h"
#include "generic_data_table.h"
#include "tuple_schema.h"
#include "correlation_common.h"

const double INVALID_DOUBLE = std::numeric_limits<double>::max();

class CorrelationIndex {

  struct AttributePair {
    AttributePair() : guest_(0), host_(0), tuple_id_(0) {}
    AttributePair(const uint64_t target, const uint64_t host, const uint64_t tuple_id) : guest_(target), host_(host), tuple_id_(tuple_id) {}

    uint64_t guest_;
    uint64_t host_;
    uint64_t tuple_id_;
  };

  static bool compare_func(AttributePair &lhs, AttributePair &rhs) {
    return lhs.guest_ < rhs.guest_;
  }

  class CorrelationNode {

  public:

    CorrelationNode(CorrelationIndex *index, const uint64_t offset_begin, const uint64_t offset_end, const size_t level) {
      
      index_ = index;

      offset_begin_ = offset_begin;
      offset_end_ = offset_end;
      offset_span_ = offset_end_ - offset_begin_ + 1;
      child_offset_span_ = offset_span_ / index_->params_.fanout_;

      auto *container_ptr = index_->container_;
      guest_begin_ = container_ptr[offset_begin_].guest_;
      guest_end_ = container_ptr[offset_end_].guest_;

      // these are useful only when calculating slope using interpolation
      host_begin_ = container_ptr[offset_begin_].host_;
      host_end_ = container_ptr[offset_end_].host_;
      
      children_ = nullptr;
      children_count_ = 0;

      slope_ = INVALID_DOUBLE;
      intercept_ = INVALID_DOUBLE;

      double density = std::abs(offset_span_ * 1.0 / (host_end_ - host_begin_));
      epsilon_ = std::ceil(index_->params_.error_bound_ * 1.0 / density / 2);

      // epsilon_ = (host_end_ - host_begin_) * index_->params_.error_bound_;

      level_ = level;

      compute_enabled_ = false;
    }

    ~CorrelationNode() {
      if (children_ != nullptr) {
        for (size_t i = 0; i < children_count_; ++i) {
          delete children_[i];
          children_[i] = nullptr;
        }
        delete[] children_;
        children_ = nullptr;

        delete[] children_index_;
        children_index_ = nullptr;
      }
    }

    void split(CorrelationNode** &new_nodes) {
      ASSERT(children_ == nullptr && children_count_ == 0, "children must be unassigned");

      outlier_buffer_.clear();

      auto *container_ptr = index_->container_;
      if (level_ == index_->params_.max_height_ - 1) {

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, container_ptr[i].tuple_id_} );
        }
        return;
      }

      children_count_ = index_->params_.fanout_;
      
      children_ = new CorrelationNode*[children_count_];
      children_index_ = new uint64_t[children_count_];

      for (size_t i = 0; i < children_count_ - 1; ++i) {
        children_[i] = new CorrelationNode(index_, offset_begin_ + child_offset_span_ * i, offset_begin_ + child_offset_span_ * (i + 1) - 1, level_ + 1);
        children_index_[i] = container_ptr[offset_begin_ + child_offset_span_ * (i + 1)].guest_;
      }
      children_[children_count_ - 1] = new CorrelationNode(index_, offset_begin_ + child_offset_span_ * (children_count_ - 1), offset_end_, level_ + 1);
      
      new_nodes = children_;
    }

    // if true, then computed. otherwise, all data are in outlier buffer.
    bool compute_interpolation() {

      if (offset_span_ <= index_->params_.min_node_size_) {

        auto *container_ptr = index_->container_;

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, container_ptr[i].tuple_id_} );
        }
        return false;
      }

      ASSERT(guest_end_ != guest_begin_, "guest_end_ cannot be equal to guest_begin_");

      slope_ = (host_end_ - host_begin_) * 1.0 / (guest_end_ - guest_begin_);
      intercept_ = host_begin_ - slope_ * guest_begin_;

      return true;
    }

    bool compute_regression() {

      auto *container_ptr = index_->container_;

      if (offset_span_ <= index_->params_.min_node_size_) {

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, container_ptr[i].tuple_id_} );
        }
        return false;
      }

      double guest_avg = 0.0;
      double host_avg = 0.0;
      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
        guest_avg += container_ptr[i].guest_;
        host_avg += container_ptr[i].host_;
      }
      guest_avg = guest_avg * 1.0 / offset_span_;
      host_avg = host_avg * 1.0 / offset_span_;

      double upper = 0.0;
      double lower = 0.0;
      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
        upper += (container_ptr[i].guest_ - guest_avg) * (container_ptr[i].host_ - host_avg);
        lower += (container_ptr[i].guest_ - guest_avg) * (container_ptr[i].guest_ - guest_avg);
      }
      slope_ = upper * 1.0 / lower;
      intercept_ = host_avg - slope_ * guest_avg;

      return true;
    }

    inline uint64_t estimate(const uint64_t key) const {
      return slope_ * key * 1.0 + intercept_;
    }

    inline void get_bound(const uint64_t key, uint64_t &ret_key_lhs, uint64_t &ret_key_rhs) const {
      if (key < epsilon_) {
        ret_key_lhs = 0;
      } else {
        ret_key_lhs = key - epsilon_;
      }
      ret_key_rhs = key + epsilon_;
    }

    inline void get_bound(const uint64_t key0, const uint64_t key1, uint64_t &ret_key_lhs, uint64_t &ret_key_rhs) const {
      
      uint64_t small_key, large_key;
      if (key0 < key1) {
        small_key = key0;
        large_key = key1;
      } else {
        small_key = key1;
        large_key = key0;
      }

      if (small_key < epsilon_) {
        ret_key_lhs = 0;
      } else {
        ret_key_lhs = small_key - epsilon_;
      }
      ret_key_rhs = large_key + epsilon_;

      return;
    }

    // if true, then pass validation. there's no need to further split this node.
    // this function will detect outliers.
    bool validate() {
      
      assert(outlier_buffer_.size() == 0);

      auto *container_ptr = index_->container_;

      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
        uint64_t guest = container_ptr[i].guest_;
        uint64_t host = container_ptr[i].host_;

        uint64_t estimate_host = estimate(guest);

        uint64_t estimate_host_lhs, estimate_host_rhs;

        get_bound(estimate_host, estimate_host_lhs, estimate_host_rhs);

        if (estimate_host_lhs > host || estimate_host_rhs < host) {
          // if not in the range
          // std::cout << estimate_host_lhs << " " << host << " " << estimate_host_rhs << std::endl;
          uint64_t tuple_id = container_ptr[i].tuple_id_;
          outlier_buffer_.insert( {guest, tuple_id} );
        }
      }

      if (outlier_buffer_.size() > offset_span_ * index_->params_.outlier_threshold_) {

        // std::cout << "validation failed: " << outlier_buffer_.size() << " " << offset_span_ << " " << offset_span_ * index_->outlier_threshold_ << std::endl;
        // std::cout << "param: " << slope_ << " " << intercept_ << std::endl;
        compute_enabled_ = false;
        return false;
      } else {
        // compute_enabled_ is set to true only if the node passed the validation.
        compute_enabled_ = true;
        return true;
      }

    }

    // return true means have range
    bool lookup(const uint64_t ip_guest, uint64_t &ret_host_lhs, uint64_t &ret_host_rhs, std::vector<uint64_t> &outliers) const {
      
      if (ip_guest < guest_begin_ || ip_guest > guest_end_) { return false; }
      
      if (children_count_ == 0) {
        // this is leaf node. search here.

        // first check outlier_buffer
        auto ret = outlier_buffer_.equal_range(ip_guest);
        for (auto it = ret.first; it != ret.second; ++it) {
          outliers.push_back(it->second);
        }

        if (compute_enabled_ == true) {
          // std::cout << "compute enabled: " << outlier_buffer_.size() << " " << offset_span_ << " " << epsilon_ << std::endl;
          // estimate the host key via function computation
          uint64_t estimate_host = estimate(ip_guest);
          // get min and max bound based on estimated value
          get_bound(estimate_host, ret_host_lhs, ret_host_rhs);

          return true;

        } else {

          return false;

        }

      } else {
        // TODO: accelerate using SIMD
        for (size_t i = 0; i < children_count_ - 1; ++i) {
          
          if (ip_guest < children_index_[i]) {

            return children_[i]->lookup(ip_guest, ret_host_lhs, ret_host_rhs, outliers);
          }
        }

        return children_[children_count_ - 1]->lookup(ip_guest, ret_host_lhs, ret_host_rhs, outliers);
      }
    }

    void range_lookup(const uint64_t ip_guest_lhs, const uint64_t ip_guest_rhs, std::vector<std::pair<uint64_t, uint64_t>> &ret_host_ranges, std::vector<uint64_t> &outliers) const {

      assert(ip_guest_lhs < ip_guest_rhs);

      if (ip_guest_rhs < guest_begin_ || ip_guest_lhs > guest_end_) { return; }

      uint64_t real_guest_lhs = (ip_guest_lhs > guest_begin_) ? ip_guest_lhs : guest_begin_;
      uint64_t real_guest_rhs = (ip_guest_rhs < guest_end_) ? ip_guest_rhs : guest_end_;

      if (children_count_ == 0) {
        // this is leaf node. search here.

        // first check outlier_buffer
        auto begin_iter = outlier_buffer_.lower_bound(real_guest_lhs);
        auto end_iter = outlier_buffer_.upper_bound(real_guest_rhs);
        for (auto it = begin_iter; it != end_iter; ++it) {
          outliers.push_back(it->second);
        }

        if (compute_enabled_ == true) {

          // estimate the host key via function computation
          uint64_t estimate_host_lhs = estimate(real_guest_lhs);
          uint64_t estimate_host_rhs = estimate(real_guest_rhs);

          uint64_t ret_host_lhs, ret_host_rhs;
          // get min and max bound based on estimated value
          get_bound(estimate_host_lhs, estimate_host_rhs, ret_host_lhs, ret_host_rhs);

          ret_host_ranges.push_back( { ret_host_lhs, ret_host_rhs } );

          return;

        } else {

          return;

        }

      } else {
        // TODO: accelerate using SIMD
        for (size_t i = 0; i < children_count_ - 1; ++i) {
          
          if (real_guest_lhs < children_index_[i]) {

            children_[i]->range_lookup(real_guest_lhs, real_guest_rhs, ret_host_ranges, outliers);
          }

          if (real_guest_rhs < children_index_[i]) {

            return;

          }
        }

        children_[children_count_ - 1]->range_lookup(real_guest_lhs, real_guest_rhs, ret_host_ranges, outliers);

        return;

      }
    }

    inline bool has_children() const {
      return (children_ != nullptr);
    }

    inline CorrelationNode** get_children() const {
      return children_;
    }

    void print(std::ofstream &outfile) const {

      if (slope_ != INVALID_DOUBLE && intercept_ != INVALID_DOUBLE) {
        outfile << level_ << "," 
                << guest_begin_ << "," 
                << guest_end_ << "," 
                << host_begin_ << "," 
                << host_end_ << "," 
                << offset_span_ << "," 
                << epsilon_ << "," 
                << slope_ << "," 
                << intercept_ << "," 
                << outlier_buffer_.size() << ","
                << (children_count_ == 0) << ","
                << compute_enabled_
                << std::endl;
      } else {
        outfile << level_ << "," 
                << guest_begin_ << "," 
                << guest_end_ << "," 
                << epsilon_ << ",NA,NA," 
                << outlier_buffer_.size() << ","
                << (children_count_ == 0) << ","
                << compute_enabled_
                << std::endl;
      }
      
    }

    inline size_t get_level() const { return level_; }

  private:

    CorrelationIndex *index_;

    size_t level_;

    // offset range: [offset_begin_, offset_end_]
    uint64_t offset_begin_;
    uint64_t offset_end_;

    uint64_t offset_span_;
    uint64_t child_offset_span_;
    
    // guest range: [guest_begin_, guest_end_]
    uint64_t guest_begin_; // guest_values[offset_begin_]
    uint64_t guest_end_; // guest_values[offset_end_]

    // host range: [host_begin_, host_end_]
    uint64_t host_begin_; // host_values[offset_begin_]
    uint64_t host_end_; // host_values[offset_end_]

    double slope_;
    double intercept_;
    bool compute_enabled_;

    double epsilon_;

    CorrelationNode **children_;
    size_t children_count_;
    uint64_t *children_index_;

    std::multimap<uint64_t, uint64_t> outlier_buffer_;
  };

public:
  CorrelationIndex(const CorrelationIndexParams &params) {

    ASSERT(params.fanout_ >= 2, "fanout must be no less than 2");

    params_ = params;

    max_level_ = 0;
    node_count_ = 0;

  }

  ~CorrelationIndex() {

    if (container_ != nullptr) {
      delete[] container_;
      container_ = nullptr;
    }

    if (root_node_ != nullptr) {
      delete root_node_;
      root_node_ = nullptr;
    }

  }

  bool lookup(const uint64_t ip_guest, uint64_t &ret_host_lhs, uint64_t &ret_host_rhs, std::vector<uint64_t> &outliers) const {

    ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

    return root_node_->lookup(ip_guest, ret_host_lhs, ret_host_rhs, outliers);
  }

  void range_lookup(const uint64_t ip_guest_lhs, const uint64_t ip_guest_rhs, std::vector<std::pair<uint64_t, uint64_t>> &ret_host_ranges, std::vector<uint64_t> &outliers) const {

    ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

    root_node_->range_lookup(ip_guest_lhs, ip_guest_rhs, ret_host_ranges, outliers);
  }

  void construct(const GenericDataTable *data_table, const TupleSchema &tuple_schema, const size_t guest_column_id, const size_t host_column_id, const IndexPointerType index_pointer_type) {

    size_ = data_table->size();

    container_ = new AttributePair[size_];

    GenericDataTableIterator iterator(data_table);
    size_t i = 0;

    if (index_pointer_type == LogicalPointerType) {
      
      while (iterator.has_next()) {
        auto entry = iterator.next();
        
        char *tuple_ptr = entry.key_;

        uint64_t pkey = *(uint64_t*)tuple_ptr;

        size_t guest_attr_offset = tuple_schema.get_attr_offset(guest_column_id);
        uint64_t guest_attr_ret = *(uint64_t*)(tuple_ptr + guest_attr_offset);

        size_t host_attr_offset = tuple_schema.get_attr_offset(host_column_id);
        uint64_t host_attr_ret = *(uint64_t*)(tuple_ptr + host_attr_offset);

        container_[i].guest_ = guest_attr_ret;
        container_[i].host_ = host_attr_ret;
        container_[i].tuple_id_ = pkey;
        // std::cout << "tuple id = " << pkey << std::endl;
        i++;
      }

    } else {

      while (iterator.has_next()) {
        auto entry = iterator.next();
        
        char *tuple_ptr = entry.key_;

        uint64_t offset = entry.offset_;

        size_t guest_attr_offset = tuple_schema.get_attr_offset(guest_column_id);
        uint64_t guest_attr_ret = *(uint64_t*)(tuple_ptr + guest_attr_offset);

        size_t host_attr_offset = tuple_schema.get_attr_offset(host_column_id);
        uint64_t host_attr_ret = *(uint64_t*)(tuple_ptr + host_attr_offset);

        container_[i].guest_ = guest_attr_ret;
        container_[i].host_ = host_attr_ret;
        container_[i].tuple_id_ = offset;
        i++;
      }

    }

    ASSERT(i == size_, "incorrect loading");

    // sort data
    std::sort(container_, container_ + size_, compare_func);

    root_node_ = new CorrelationNode(this, 0, size_ - 1, 0);

    std::queue<CorrelationNode*> nodes;
    nodes.push(root_node_);

    while (!nodes.empty()) {
      auto *node = nodes.front();
      nodes.pop();

      node_count_++;

      if (node->get_level() >= max_level_) {
        max_level_ = node->get_level();
      }

      bool compute_ret;
      if (params_.compute_type_ == InterpolationType) {
        compute_ret = node->compute_interpolation();
      } else {
        compute_ret = node->compute_regression();
      }

      if (compute_ret == true) {

        bool validate_ret = node->validate();
        if (validate_ret == false) {
          CorrelationNode** new_nodes = nullptr;
          node->split(new_nodes);
          if (new_nodes != nullptr) {
            for (size_t i = 0; i < params_.fanout_; i++) {
              nodes.push(new_nodes[i]);
            }
          }
        }
      }
    }

    delete[] container_;
    container_ = nullptr;
  }

  void print(const bool verbose = false) const {

    std::cout << "max level = " << max_level_ << std::endl;
    std::cout << "node count = " << node_count_ << std::endl;

    if (verbose == true) {

      std::ofstream outfile("index.txt");

      if (outfile.is_open() == false) { assert(false); }

      ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

      std::queue<CorrelationNode*> nodes;

      nodes.push(root_node_);

      while (!nodes.empty()) {
        auto *node = nodes.front();
        nodes.pop();

        node->print(outfile);

        CorrelationNode** next_nodes = node->get_children();

        if (next_nodes != nullptr) {
          for (size_t i = 0; i < params_.fanout_; ++i) {
            nodes.push(next_nodes[i]);
          }
        }
      }

      outfile.close();
    }

  }

private:

  AttributePair *container_;
  size_t size_;

  CorrelationIndexParams params_;

  CorrelationNode *root_node_;

  size_t max_level_;
  size_t node_count_;

};
