#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

#include "generic_key.h"
#include "generic_data_table.h"

#include "utils.h"

class SST {

struct Entry {
  Entry(const GenericKey &key, const GenericKey &payload, const uint64_t timestamp) {
    key_ = key;
    payload_ = payload;
    timestamp_ = timestamp;
  }

  GenericKey key_;
  GenericKey payload_;
  uint64_t timestamp_;
};

public:
  SST(const size_t key_size, const size_t payload_size, const size_t sst_id) : key_size_(key_size), payload_size_(payload_size), sst_id_(sst_id) {}
  ~SST() {}

  void add(const GenericKey &key, const GenericKey &payload, const uint64_t timestamp) {
    entries_.push_back( {key, payload, timestamp} );
  }

  void persist() {
    size_t entry_size = key_size_ + payload_size_ + sizeof(uint64_t);
    size_t total_size = entry_size * entries_.size();
    
    char *mem_buffer = new char[total_size];
    size_t offset = 0;

    for (auto& entry : entries_) {
      memcpy(mem_buffer + offset, entry.key_.raw(), key_size_);
      offset += key_size_;
      memcpy(mem_buffer + offset, &entry.timestamp_, sizeof(uint64_t));
      offset += sizeof(uint64_t);
      memcpy(mem_buffer + offset, entry.payload_.raw(), payload_size_);
      offset += payload_size_;
    }

    assert(offset == total_size);

    write_file(sst_id_, total_size, mem_buffer);

    delete[] mem_buffer;
    mem_buffer = nullptr;
  }

  void write_file(const size_t file_id, const size_t size, const char *data) {
    std::string filename = get_filename(file_id);
    
    FILE *file = fopen(filename.c_str(), "w+b");
    size_t ret = fwrite(data, size, 1, file);
    assert(ret == 1);
    fflush(file);
    fsync(fileno(file));
    fclose(file);
  }

  void read_file(const size_t file_id, size_t size, char *data) const {
    std::string filename = get_filename(file_id);

    FILE *file = fopen(filename.c_str(), "r+b");
    fseek(file, 0, SEEK_SET);
    int ret = fread(data, size, 1, file);
    assert(ret == 1);
    fclose(file);
  }

  std::string get_filename(const size_t file_id) const {
    return std::string("sst" + std::to_string(file_id) + ".dat");
  }

  std::vector<Entry> entries_;

  size_t key_size_;
  size_t payload_size_;
  size_t sst_id_;
};


class LSMTrie {

public:
  LSMTrie(const size_t key_size, const size_t payload_size) : key_size_(key_size), payload_size_(payload_size), sst_id_(0), fanout_(8) {}
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

    SST **ssts = new SST*[fanout_];

    for (size_t i = 0; i < fanout_; ++i) {
      ssts[i] = new SST(key_size_, payload_size_, sst_id_);
      sst_id_++;
    }

    uint64_t barrier = 256 / fanout_;
    for (auto& entry : imm_buffer_) {

      ssts[uint8_t(entry.first.raw()[0]) / barrier]->add(entry.first, entry.second.first, entry.second.second);

    }

    for (size_t i = 0; i < fanout_; ++i) {
      delete[] ssts[i];
      ssts[i] = nullptr;
    }

    delete[] ssts;
    ssts = nullptr;
  }

private:
  std::multimap<GenericKey, std::pair<GenericKey, uint64_t>> imm_buffer_;
  
  size_t key_size_;
  size_t payload_size_;
  size_t sst_id_;

  size_t fanout_;
};
