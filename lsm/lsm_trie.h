#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

#include "generic_key.h"
#include "generic_data_table.h"

#include "utils.h"

class LSMTrie {

public:
  LSMTrie() {}
  ~LSMTrie() {}
  
  void insert(const GenericKey &key, const GenericKey &payload, const uint64_t timestamp) {

    imm_buffer_.insert( {key, {payload, timestamp} } );
  }

  void lookup(const GenericKey &key, const uint64_t timestamp, std::vector<GenericKey> &ret_payloads) {

    auto iter = imm_buffer_.equal_range(key);
    for (auto it = iter.first; it != iter.second; ++it) {
      ret_payloads.push_back(it->second.first);
    }
  }

  void persist() {
    
    // std::vector<GenericKey, std::pair<GenericKey, uint64_t>> persist_buffers_[fanout_];
    uint64_t barrier = 256 / fanout_;
    for (auto& entry : imm_buffer_) {

      // uint8_t(entry.first.raw()[0]);

    }
  }

private:
  std::multimap<GenericKey, std::pair<GenericKey, uint64_t>> imm_buffer_;
  size_t fanout_ = 8;
};
