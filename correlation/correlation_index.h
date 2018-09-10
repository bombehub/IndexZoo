#pragma once

#include <vector>
#include <map>


class BaseCorrelationIndex {
public:
  BaseCorrelationIndex() {}
  virtual ~BaseCorrelationIndex() {}

  virtual void construct(std::vector<uint64_t> &target_column, std::vector<uint64_t> &host_column) = 0;

  virtual void lookup(const uint64_t key, std::vector<uint64_t> &ret_keys) = 0;
};



class FullCorrelationIndex : public BaseCorrelationIndex {
public:
  FullCorrelationIndex() {}
  virtual ~FullCorrelationIndex() {}

  virtual void construct(std::vector<uint64_t> &target_column, std::vector<uint64_t> &host_column) final {
    assert(target_column.size() == host_column.size());

    for (size_t i = 0; i < target_column.size(); ++i) {
      corr_index_.insert( { target_column.at(i), host_column.at(i) } );
    }
  }

  virtual void lookup(const uint64_t key, std::vector<uint64_t> &ret_keys) final {
    auto ret = corr_index_.equal_range(key);
    for (auto it = ret.first; it != ret.second; ++it) {
      ret_keys.push_back(it->second);
    }
  }

private:
  std::multimap<uint64_t, uint64_t> corr_index_;

};

struct AttributePair {
  AttributePair() : target_(0), host_(0) {}
  AttributePair(const uint64_t target, const uint64_t host) : target_(target), host_(host) {}

  uint64_t target_;
  uint64_t host_;
};

static bool compare_func(AttributePair &lhs, AttributePair &rhs) {
  return lhs.target_ < rhs.target_;
}


class InterpolationCorrelationIndex : public BaseCorrelationIndex {
public:
  InterpolationCorrelationIndex() : container_(nullptr), size_(0) {}
  virtual ~InterpolationCorrelationIndex() {
    if (container_ != nullptr) {
      delete[] container_;
      container_ = nullptr;
    }
  }

  virtual void construct(std::vector<uint64_t> &target_column, std::vector<uint64_t> &host_column) final {
    assert(target_column.size() == host_column.size());

    size_ = target_column.size();

    container_ = new AttributePair[size_];

    for (size_t i = 0; i < target_column.size(); ++i) {
      container_[i].target_ = target_column.at(i);
      container_[i].host_ = host_column.at(i);
    }

    std::sort(container_, container_ + size_, compare_func);

  }

  virtual void lookup(const uint64_t key, std::vector<uint64_t> &ret_keys) final {
    for (size_t i = 0; i < size_; ++i) {
      if (container_[i].target_ == key) {
        ret_keys.push_back(container_[i].host_);
      }
      if (container_[i].target_ > key) {
        return;
      }
    }
  }

  AttributePair *container_;
  size_t size_;

};