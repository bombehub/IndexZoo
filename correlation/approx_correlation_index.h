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

  struct CorrelationNode {

    CorrelationNode(ApproxCorrelationIndex *index, const uint64_t offset_begin, const uint64_t offset_end) {
      offset_begin_ = offset_begin;
      offset_end_ = offset_end;
      offset_span_ = offset_end_ - offset_begin_;

      auto *container_ptr = index->container_;
      guest_begin_ = container_ptr[offset_begin_].guest_;
      guest_end_ = container_ptr[offset_end_].guest_;

      host_begin_ = container_ptr[offset_begin_].host_;
      host_end_ = container_ptr[offset_end_].host_;
      
      children_ = nullptr;
      children_count_ = 0;
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

    void split(ApproxCorrelationIndex *index, CorrelationNode** new_nodes) {
      assert(children_ == nullptr);
      children_count_ = index->fanout_;
      
      children_ = new CorrelationNode*[children_count_];
      children_index_ = new uint64_t[children_count_];

      for (size_t i = 0; i < children_count_ - 1; ++i) {
        children_[i] = new CorrelationNode(index, offset_begin_ + offset_span_ * i, offset_begin_ + offset_span_ * (i + 1) - 1);
        children_index_[i] = index->container_[offset_begin_ + offset_span_ * (i + 1)].guest_;
      }
      children_[children_count_ - 1] = new CorrelationNode(index, offset_begin_ + offset_span_ * (children_count_ - 1), offset_end_);
      
      new_nodes = children_;
    }

    bool validate(ApproxCorrelationIndex *index) {
      for (uint64_t i = offset_begin_; i <= offset_end_; ++i) {

      }
      return true;

    }

    void lookup(const uint64_t guest_key, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host) {
      if (children_count_ == 0) {
        uint64_t host_key = (host_end_ - host_begin_) * 1.0 / (guest_end_ - guest_begin_) * (guest_key - guest_begin_) + host_begin_;
        ret_lhs_host = host_key;
        ret_rhs_host = host_key;
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

    // offset range: [offset_begin_, offset_end_]
    uint64_t offset_begin_;
    uint64_t offset_end_;

    uint64_t offset_span_;
    
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

  }

  ~ApproxCorrelationIndex() {

    delete[] container_;
    container_ = nullptr;

    delete root_node_;
    root_node_ = nullptr;

  }

  void lookup(const uint64_t guest, uint64_t &ret_lhs_host, uint64_t &ret_rhs_host) {
    assert(root_node_ != nullptr);
    root_node_->lookup(guest, ret_lhs_host, ret_rhs_host);
  }

  void construct(std::vector<uint64_t> &guest_column, std::vector<uint64_t> &host_column) {

    assert(guest_column.size() == host_column.size());

    size_ = guest_column.size();

    container_ = new AttributePair[size_];

    for (size_t i = 0; i < size_; ++i) {
      container_[i].guest_ = guest_column.at(i);
      container_[i].host_ = host_column.at(i);
    }

    // sort data
    std::sort(container_, container_ + size_, compare_func);

    // std::queue<CorrelationNode*> nodes;

    root_node_ = new CorrelationNode(this, 0, size_ - 1);

    CorrelationNode** new_nodes = nullptr;
    root_node_->split(this, new_nodes);


    // nodes.push(root_node_);

    // while (!nodes.empty()) {
    //   auto *node = nodes.front();
    //   nodes.pop();

    //   bool ret = node->validate(this);

    //   if (ret == false) {
    //     CorrelationNode** new_nodes = nullptr;
    //     node->split(this, new_nodes);
    //     for (size_t i = 0; i < fanout_; i++) {
    //       nodes.push(new_nodes[i]);
    //     }
    //   }

    // }
  }

private:

  AttributePair *container_;
  size_t size_;

  size_t fanout_;
  float error_bound_;
  float outlier_threshold_;

  CorrelationNode *root_node_;

};
