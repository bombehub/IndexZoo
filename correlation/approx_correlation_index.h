#pragma once

#include <vector>
#include <map>
#include <unordered_map>
#include <queue>


class ApproxCorrelationIndex {

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

    CorrelationNode(ApproxCorrelationIndex *index, const uint64_t offset_begin, const uint64_t offset_end, const size_t level) {
      offset_begin_ = offset_begin;
      offset_end_ = offset_end;
      offset_span_ = offset_end_ - offset_begin_ + 1;
      child_offset_span_ = offset_span_ / index->fanout_;

      auto *container_ptr = index->container_;
      guest_begin_ = container_ptr[offset_begin_].guest_;
      guest_end_ = container_ptr[offset_end_].guest_;

      host_begin_ = container_ptr[offset_begin_].host_;
      host_end_ = container_ptr[offset_end_].host_;
      
      children_ = nullptr;
      children_count_ = 0;

      level_ = level;
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

    void split(ApproxCorrelationIndex *index, CorrelationNode** &new_nodes) {
      ASSERT(children_ == nullptr && children_count_ == 0, "children must be unassigned");
      children_count_ = index->fanout_;
      
      children_ = new CorrelationNode*[children_count_];
      children_index_ = new uint64_t[children_count_];

      for (size_t i = 0; i < children_count_ - 1; ++i) {
        children_[i] = new CorrelationNode(index, offset_begin_ + child_offset_span_ * i, offset_begin_ + child_offset_span_ * (i + 1) - 1, level_ + 1);
        children_index_[i] = index->container_[offset_begin_ + child_offset_span_ * (i + 1)].guest_;
      }
      children_[children_count_ - 1] = new CorrelationNode(index, offset_begin_ + child_offset_span_ * (children_count_ - 1), offset_end_, level_ + 1);
      
      new_nodes = children_;

      outlier_buffer_.clear();
    }

    bool validate(ApproxCorrelationIndex *index) {
      auto *container_ptr = index->container_;

      if (offset_span_ <= 10) {
        for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
          uint64_t guest = container_ptr[i].guest_;
          uint64_t host = container_ptr[i].host_;
          outlier_buffer_[guest] = host;
        }
        return true;
      }

      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {
        uint64_t guest = container_ptr[i].guest_;
        uint64_t host = container_ptr[i].host_;

        uint64_t estimate_host = estimate(guest);

        uint64_t estimate_lhs_host, estimate_rhs_host;

        get_bound(estimate_host, estimate_lhs_host, estimate_rhs_host);

        if (estimate_lhs_host > host || estimate_rhs_host < host) {
          // if not in the range
          outlier_buffer_[guest] = host;
        }
      }
      
      if (outlier_buffer_.size() > offset_span_ * index->outlier_threshold_) {
        return false;
      } else {
        return true;
      }

    }

    void lookup(const uint64_t guest_key, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host) const {
      if (children_count_ == 0) {

        // first check outlier_buffer
        auto iter = outlier_buffer_.find(guest_key);
        if (iter != outlier_buffer_.end()) {

          uint64_t host_key = iter->second;

          ret_lhs_host = host_key;
          ret_rhs_host = host_key;

          return;

        } else {
          // estimate the host key via function computation
          uint64_t host_key = estimate(guest_key);
          // get min and max bound based on estimated value
          get_bound(host_key, ret_lhs_host, ret_rhs_host);

          return;
        }

      } else {
        for (size_t i = 0; i < children_count_ - 1; ++i) {
          if (guest_key < children_index_[i]) {
            children_[i]->lookup(guest_key, ret_lhs_host, ret_rhs_host);
            return;
          }
        }
        children_[children_count_ - 1]->lookup(guest_key, ret_lhs_host, ret_rhs_host);
        return;
      }
    }

    inline uint64_t estimate(const uint64_t key) const {
      ASSERT(guest_end_ != guest_begin_, "guest_end_ cannot be equal to guest_begin_");
      return (host_end_ - host_begin_) * 1.0 / (guest_end_ - guest_begin_) * (key - guest_begin_) + host_begin_;
    }

    inline void get_bound(const uint64_t key, uint64_t &lhs_key, uint64_t &rhs_key) const {
      uint64_t epsilon = 20;
      if (key < epsilon) {
        lhs_key = 0;
      } else {
        lhs_key = key - epsilon;
      }
      lhs_key = key - epsilon;
      rhs_key = key + epsilon;      
    }

    inline bool has_children() const {
      return (children_ != nullptr);
    }

    inline CorrelationNode** get_children() const {
      return children_;
    }

    void print() const {
      std::cout << "offset span: " << offset_span_ << std::endl;
      std::cout << "offset: " << offset_begin_ << " " << offset_end_ << std::endl;
      std::cout << "guest: " << guest_begin_ << " " << guest_end_ << std::endl;
      std::cout << "host: " << host_begin_ << " " << host_end_ << std::endl;
      std::cout << "======" << std::endl;
    }

    size_t get_level() const {
      return level_;
    }

  private:

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


    CorrelationNode **children_;
    size_t children_count_;
    uint64_t *children_index_;

    std::unordered_map<uint64_t, uint64_t> outlier_buffer_;
  };

public:
  ApproxCorrelationIndex(const size_t fanout = 2, const float error_bound = 0.1, const float outlier_threshold = 0.2) {

    ASSERT(fanout >= 2, "fanout must be no less than 2");

    fanout_ = fanout;
    error_bound_ = error_bound;
    outlier_threshold_ = outlier_threshold;

    max_level_ = 0;
    node_count_ = 0;

  }

  ~ApproxCorrelationIndex() {

    if (container_ != nullptr) {
      delete[] container_;
      container_ = nullptr;
    }

    if (root_node_ != nullptr) {
      delete root_node_;
      root_node_ = nullptr;
    }

  }

  void lookup(const uint64_t guest, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host) {
    ASSERT(root_node_ != nullptr, "root note cannot be nullptr");
    root_node_->lookup(guest, ret_lhs_host, ret_rhs_host);
  }

  void construct(const std::vector<uint64_t> &guest_column, const std::vector<uint64_t> &host_column) {

    ASSERT(guest_column.size() == host_column.size(), "guest and host columns must have same cardinality");

    size_ = guest_column.size();

    container_ = new AttributePair[size_];

    for (size_t i = 0; i < size_; ++i) {
      container_[i].guest_ = guest_column.at(i);
      container_[i].host_ = host_column.at(i);
    }

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

      bool ret = node->validate(this);

      if (ret == false) {
        CorrelationNode** new_nodes = nullptr;
        node->split(this, new_nodes);
        for (size_t i = 0; i < fanout_; i++) {
          nodes.push(new_nodes[i]);
        }
      }

    }

    delete[] container_;
    container_ = nullptr;
  }

  void print(const bool verbose = false) const {

    if (verbose == false) { 

      std::cout << "max level = " << max_level_ << std::endl;
      std::cout << "node count = " << node_count_ << std::endl;

    } else {

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

  size_t fanout_;
  float error_bound_;
  float outlier_threshold_;

  CorrelationNode *root_node_;

  size_t max_level_;
  size_t node_count_;

};
