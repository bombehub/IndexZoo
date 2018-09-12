#pragma once

#include <vector>

#include "dynamic_index/singlethread/stx_btree/btree_multimap.h"

class BTreeIndex {
public:

  BTreeIndex() {}
  ~BTreeIndex() {}

  void insert(const uint64_t key, const uint64_t value) {
    container_.insert( {key, value} );
  }

  void lookup(const uint64_t key, std::vector<uint64_t> &values) {
    auto ret = container_.equal_range(key);
    for (auto it = ret.first; it != ret.second; ++it) {
      values.push_back(it->second);
    }
  }

  void lookup(const std::vector<uint64_t> &keys, std::vector<uint64_t> &values) {
    for (auto key : keys) {
      auto ret = container_.equal_range(key);
      for (auto it = ret.first; it != ret.second; ++it) {
        values.push_back(it->second);
      }
    }
  }

  void range_lookup(const uint64_t &lhs_key, const uint64_t &rhs_key, std::vector<uint64_t> &values) {
    
    if (lhs_key > rhs_key) { return; }

    auto itlow = container_.lower_bound(lhs_key);
    auto itup = container_.upper_bound(rhs_key);

    for (auto it = itlow; it != itup; ++it) {
      values.push_back(it->second);
    }
  }

private:
  stx::btree_multimap<uint64_t, uint64_t> container_;
};