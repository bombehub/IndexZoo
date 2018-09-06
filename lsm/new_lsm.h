#pragma once

#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <unistd.h>

#include "generic_key.h"

const uint64_t MIN_TS = 0;
const uint64_t MAX_TS = std::numeric_limits<uint64_t>::max();

const size_t WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
// const size_t MAX_FILE_SIZE = WRITE_BUFFER_SIZE * 2;
// const size_t MAX_KEY_SPACE_COUNT = 4;
// const size_t MAX_LEVEL_DEPTH = 10;


typedef int(*imm_callback)(void *data, const unsigned char *key, const size_t key_size, const unsigned char *payload, const size_t payload_size, void *data);

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

  void iterate(imm_callback cb, void *data) {
    for (auto entry : map_) {
      imm_callback(entry.first.raw(), entry.first.size(), entry.second.first.raw(), entry.second.first.size(), entry.second.second, data);
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
    next_file_id_ = 0;

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

  static void persist_callback(void *data, const unsigned char *key, const size_t key_size, const unsigned char *payload, const size_t payload_size, void *value) {
    WriteBuffer *buffer = (WriteBuffer*)data;
    // Slice *s = (Slice*)value;
    // uint32_t value_len = s->size_;
    // write key_len
    // memcpy(buffer->data_ + buffer->offset_, &key_len, sizeof(key_len));
    // buffer->offset_ += sizeof(key_len);
    // // write value_len
    // memcpy(buffer->data_ + buffer->offset_, &value_len, sizeof(value_len));
    // buffer->offset_ += sizeof(value_len);
    // // write key
    // memcpy(buffer->data_ + buffer->offset_, key, key_len);
    // buffer->offset_ += key_len;
    // // write value
    // memcpy(buffer->data_ + buffer->offset_, s->data_, value_len);
    // buffer->offset_ += value_len;
  }

  void persist_imm_table() {

    size_t file_id = next_file_id_;
    next_file_id_++;

    imm_table_->iterate(&LSM::persist_callback, (void*)(&write_buffer_));
  }

  void write_sst(const size_t file_id, const size_t size, const char *data) {
    std::string filename = get_filename(file_id);
    
    FILE *file = fopen(filename.c_str(), "w+b");
    size_t ret = fwrite(data, size, 1, file);
    assert(ret == 1);
    fflush(file);
    fsync(fileno(file));
    fclose(file);
  }

  void read_sst(const size_t file_id, size_t size, char *data) const {
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

private:
  ImmTable *imm_table_;

  size_t next_file_id_;

  WriteBuffer *write_buffer_;

};

