#pragma once

#include "generic_key.h"

const uint64_t MIN_TS = 0;
const uint64_t MAX_TS = std::numeric_limits<uint64_t>::max();

const size_t WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
// const size_t MAX_FILE_SIZE = WRITE_BUFFER_SIZE * 2;
// const size_t MAX_KEY_SPACE_COUNT = 4;
// const size_t MAX_LEVEL_DEPTH = 10;



class ImmTable {

public:
  ImmTable() : min_ts_(MAX_TS), max_ts_(MIN_TS) {}
  ~ImmTable() {}

  void insert(const GenericKey &key, const GenericKey &payload, const uint64_t timestamp) {
    if (min_ts_ > timestamp) {
      min_ts_ = timestamp;
    }
    if (max_ts_ < timestamp) {
      max_ts_ = timestamp;
    }

    map_.insert( {key, {payload, timestamp} } );

  }

  bool find(const GenericKey &key, const uint64_t timestamp, GenericKey &ret_payload) {
    auto iter = map_.find(key);
    if (iter != map_.end()) {
      ret_payload = iter->second.first;
      return true;
    } else {
      return false;
    }
  }

private:

  uint64_t min_ts_;
  uint64_t max_ts_;

  std::multimap<GenericKey, std::pair<GenericKey, uint64_t>> map_;

};

class WriteBuffer {

public:
  WriteBuffer(const size_t key_size, const size_t payload_size) {
    data_ = new char[WRITE_BUFFER_SIZE];
    entry_size_ = key_size + payload_size + sizeof(uint64_t);
    entry_count_ = 0;
    max_entry_count_ = WRITE_BUFFER_SIZE / entry_size_;
  }

  ~WriteBuffer() {
    delete[] data_;
    data_ = nullptr;
  }

  void reset_offset() { 
    entry_count_ = 0;
  }

private:
  char *data_;
  size_t entry_size_;
  size_t entry_count_;
  size_t max_entry_count_;
};


class LSM {
public:
  LSM(const size_t key_size, const size_t payload_size) {
    imm_table_ = new ImmTable();
    write_buffer_ = new WriteBuffer(key_size, payload_size);
  }

  ~LSM() {
    delete imm_table_;
    imm_table_ = nullptr;
    delete write_buffer_;
    write_buffer_ = nullptr;
  }


  void insert(const GenericKey &key, const GenericKey &payload, const uint64_t timestamp) {
    imm_table_->insert(key, payload, timestamp);
  }

  bool find(const GenericKey &key, const uint64_t timestamp, GenericKey &ret_payload) {
    return imm_table_->find(key, timestamp, ret_payload);
  }

private:
  ImmTable *imm_table_;
  WriteBuffer *write_buffer_;

};

