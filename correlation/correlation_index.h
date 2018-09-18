#pragma once

#include <vector>
#include <map>
#include <unordered_map>
#include <queue>

#include "generic_key.h"
#include "generic_data_table.h"
#include "tuple_schema.h"
#include "correlation_common.h"

const double INVALID_DOUBLE = std::numeric_limits<double>::max();

class CorrelationIndex {

  struct AttributePair {
    AttributePair() : guest_(0), host_(0) {}
    AttributePair(const uint64_t target, const uint64_t host) : guest_(target), host_(host) {}

    uint64_t guest_;
    uint64_t host_;
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
      child_offset_span_ = offset_span_ / index_->fanout_;

      auto *container_ptr = index_->container_;
      guest_begin_ = container_ptr[offset_begin_].guest_;
      guest_end_ = container_ptr[offset_end_].guest_;

      host_begin_ = container_ptr[offset_begin_].host_;
      host_end_ = container_ptr[offset_end_].host_;
      
      children_ = nullptr;
      children_count_ = 0;

      slope_ = INVALID_DOUBLE;
      intercept_ = INVALID_DOUBLE;

      // double density = std::abs(offset_span_ * 1.0 / (host_end_ - host_begin_));

      // epsilon_ = std::ceil(index_->error_bound_ * 1.0 / density / 2);

      epsilon_ = index_->error_bound_;

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

      // slope_ = INVALID_DOUBLE;
      // intercept_ = INVALID_DOUBLE;

      auto *container_ptr = index_->container_;
      if (level_ == index_->max_height_ - 1) {

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, host} );
        }
        return;
      }

      children_count_ = index_->fanout_;
      
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

      if (offset_span_ <= index_->min_node_size_) {

        auto *container_ptr = index_->container_;

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, host} );
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

      if (offset_span_ <= index_->min_node_size_) {

        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_.insert( {guest, host} );
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

    inline void get_bound(const uint64_t key, uint64_t &lhs_key, uint64_t &rhs_key) const {
      if (key < epsilon_) {
        lhs_key = 0;
      } else {
        lhs_key = key - epsilon_;
      }
      rhs_key = key + epsilon_;
    }

    // if true, then pass validation. there's no need to further split this node.
    bool validate() {
      
      assert(outlier_buffer_.size() == 0);

      auto *container_ptr = index_->container_;

      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
        uint64_t guest = container_ptr[i].guest_;
        uint64_t host = container_ptr[i].host_;

        uint64_t estimate_host = estimate(guest);

        uint64_t estimate_lhs_host, estimate_rhs_host;

        get_bound(estimate_host, estimate_lhs_host, estimate_rhs_host);

        if (estimate_lhs_host > host || estimate_rhs_host < host) {
          // if not in the range
          // std::cout << estimate_lhs_host << " " << host << " " << estimate_rhs_host << std::endl;
          outlier_buffer_.insert( {guest, host} );
        }
      }
      
      if (outlier_buffer_.size() > offset_span_ * index_->outlier_threshold_) {

        std::cout << "validation failed: " << outlier_buffer_.size() << " " << offset_span_ << " " << offset_span_ * index_->outlier_threshold_ << std::endl;
        std::cout << "param: " << slope_ << " " << intercept_ << std::endl;
        compute_enabled_ = false;
        return false;
      } else {
        std::cout << "validation successful!" << std::endl;
        compute_enabled_ = true;
        return true;
      }

    }

    // return true means have range
    bool lookup(const uint64_t guest_key, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host, std::vector<uint64_t> &outliers) const {
      if (children_count_ == 0) {
        // this is leaf node. search here.

        // first check outlier_buffer
        auto ret = outlier_buffer_.equal_range(guest_key);
        for (auto it = ret.first; it != ret.second; ++it) {
          outliers.push_back(it->second);
        }

        if (compute_enabled_ == true) {
          // estimate the host key via function computation
          uint64_t host_key = estimate(guest_key);
          // get min and max bound based on estimated value
          get_bound(host_key, ret_lhs_host, ret_rhs_host);

          return true;

        } else {

          return false;

        }

      } else {
        // TODO: accelerate using SIMD
        for (size_t i = 0; i < children_count_ - 1; ++i) {
          
          if (guest_key < children_index_[i]) {

            return children_[i]->lookup(guest_key, ret_lhs_host, ret_rhs_host, outliers);
          }
        }

        return children_[children_count_ - 1]->lookup(guest_key, ret_lhs_host, ret_rhs_host, outliers);
      }
    }

    void range_lookup(const uint64_t guest_lhs_key, const uint64_t guest_rhs_key, std::vector<std::pair<uint64_t, uint64_t>> &ret_host_ranges, std::vector<uint64_t> &outliers) const {
      // if (children_count_ == 0) {
      //   // this is leaf node. search here.

      //   // first check outlier_buffer
      //   auto ret = outlier_buffer_.equal_range(guest_key);
      //   for (auto it = ret.first; it != ret.second; ++it) {
      //     outliers.push_back(it->second);
      //   }

      //   if (compute_enabled_ == true) {
      //     // estimate the host key via function computation
      //     uint64_t host_key = estimate(guest_key);
      //     // get min and max bound based on estimated value
      //     get_bound(host_key, ret_lhs_host, ret_rhs_host);

      //     return true;

      //   } else {

      //     return false;

      //   }

      // } else {
      //   // TODO: accelerate using SIMD
      //   for (size_t i = 0; i < children_count_ - 1; ++i) {
          
      //     if (guest_key < children_index_[i]) {

      //       return children_[i]->lookup(guest_key, ret_lhs_host, ret_rhs_host, outliers);
      //     }
      //   }

      //   return children_[children_count_ - 1]->lookup(guest_key, ret_lhs_host, ret_rhs_host, outliers);
      // }
    }

    inline bool has_children() const {
      return (children_ != nullptr);
    }

    inline CorrelationNode** get_children() const {
      return children_;
    }

    void print() const {
      std::cout << "******" << std::endl;
      std::cout << "level: " << level_ << std::endl;
      std::cout << "offset span: " << offset_span_ << std::endl;
      std::cout << "offset: " << offset_begin_ << " " << offset_end_ << std::endl;
      std::cout << "guest: " << guest_begin_ << " " << guest_end_ << std::endl;
      std::cout << "host: " << host_begin_ << " " << host_end_ << std::endl;
      std::cout << "epsilon: " << epsilon_ << std::endl;
      if (slope_ != INVALID_DOUBLE || intercept_ != INVALID_DOUBLE) {
        std::cout << "slope: " << slope_ << ", intercept: " << intercept_ << std::endl;
      } 
      std::cout << "outlier count: " << outlier_buffer_.size() << std::endl;
      std::cout << "leaf node: " << (children_count_ == 0) << std::endl;
      std::cout << "======" << std::endl;
    }

    size_t get_level() const {
      return level_;
    }

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
  CorrelationIndex(const size_t fanout, const size_t error_bound, const float outlier_threshold, const size_t min_node_size, const size_t max_height, const ComputeType compute_type) {

    ASSERT(fanout >= 2, "fanout must be no less than 2");

    fanout_ = fanout;
    error_bound_ = error_bound;
    outlier_threshold_ = outlier_threshold;
    min_node_size_ = min_node_size;
    max_height_ = max_height;
    compute_type_ = compute_type;

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

  bool lookup(const uint64_t guest_key, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host, std::vector<uint64_t> &outliers) const {

    ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

    return root_node_->lookup(guest_key, ret_lhs_host, ret_rhs_host, outliers);
  }

  void range_lookup(const uint64_t guest_lhs_key, const uint64_t guest_rhs_key, std::vector<std::pair<uint64_t, uint64_t>> &ret_host_ranges, std::vector<uint64_t> &outliers) const {

    ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

    root_node_->range_lookup(guest_lhs_key, guest_rhs_key, ret_host_ranges, outliers);
  }

  void construct(const GenericDataTable *data_table, const TupleSchema &tuple_schema, const size_t guest_column_id, const size_t host_column_id) {

    size_ = data_table->size();

    container_ = new AttributePair[size_];

    GenericDataTableIterator iterator(data_table);
    size_t i = 0;
    while (iterator.has_next()) {
      auto entry = iterator.next();
      
      char *tuple_ptr = entry.key_;

      size_t guest_attr_offset = tuple_schema.get_attr_offset(guest_column_id);
      uint64_t guest_attr_ret = *(uint64_t*)(tuple_ptr + guest_attr_offset);

      size_t host_attr_offset = tuple_schema.get_attr_offset(host_column_id);
      uint64_t host_attr_ret = *(uint64_t*)(tuple_ptr + host_attr_offset);


      container_[i].guest_ = guest_attr_ret;
      container_[i].host_ = host_attr_ret;
      i++;
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
      if (compute_type_ == InterpolationType) {
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
            for (size_t i = 0; i < fanout_; i++) {
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

      ASSERT(root_node_ != nullptr, "root note cannot be nullptr");

      std::queue<CorrelationNode*> nodes;

      nodes.push(root_node_);

      while (!nodes.empty()) {
        auto *node = nodes.front();
        nodes.pop();

        node->print();

        CorrelationNode** next_nodes = node->get_children();

        if (next_nodes != nullptr) {
          for (size_t i = 0; i < fanout_; ++i) {
            nodes.push(next_nodes[i]);
          }
        }
      }
    }

  }

private:

  AttributePair *container_;
  size_t size_;

  //=====configurations======
  size_t fanout_;
  size_t error_bound_;
  float outlier_threshold_;
  size_t min_node_size_;
  size_t max_height_;
  ComputeType compute_type_;
  ////////////////////////////  

  CorrelationNode *root_node_;

  size_t max_level_;
  size_t node_count_;

};
