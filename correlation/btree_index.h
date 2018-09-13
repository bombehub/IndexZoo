#pragma once

#include <vector>

#include "dynamic_index/singlethread/stx_btree/btree_multimap.h"

#include "generic_key.h"
#include "generic_data_table.h"
#include "tuple_schema.h"

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

  void construct(const GenericDataTable *data_table, const TupleSchema &tuple_schema, const size_t column_id) {

    size_t capacity = data_table->size();
  
    std::pair<uint64_t, uint64_t> *columns = new std::pair<uint64_t, uint64_t>[capacity];
    
    GenericDataTableIterator iterator(data_table);
    size_t i = 0;
    while (iterator.has_next()) {
      auto entry = iterator.next();
      
      char *tuple_ptr = entry.key_;

      // Uint64 offset = entry.offset_;

      uint64_t pkey = *(uint64_t*)tuple_ptr;

      size_t attr_offset = tuple_schema.get_attr_offset(column_id);
      uint64_t attr_ret = *(uint64_t*)(tuple_ptr + attr_offset);

      columns[i].first = attr_ret;
      columns[i].second = pkey;
      i++;
    }

    ASSERT(i == capacity, "incorrect loading");

    std::sort(columns, columns + capacity);
    container_.bulk_load(columns, columns + capacity);

    delete[] columns;
    columns = nullptr;

  }

private:
  stx::btree_multimap<uint64_t, uint64_t> container_;
};