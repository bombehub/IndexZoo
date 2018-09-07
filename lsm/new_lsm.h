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


typedef void(*imm_callback)(void *data, const char *key, const size_t key_size, const char *payload, const size_t payload_size, const uint64_t timestamp);

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
      cb(data, entry.first.raw(), entry.first.size(), entry.second.first.raw(), entry.second.first.size(), entry.second.second);
      std::cout << entry.first.size() << " " << entry.second.first.size() << " " << entry.second.second << std::endl;
    }
  }

  void reset() {
    min_ts_ = MAX_TS;
    max_ts_ = MIN_TS;
    map_.clear();
  }

  uint64_t get_min_ts() const {
    return min_ts_;
  }

  uint64_t get_max_ts() const {
    return max_ts_;
  }

private:

  uint64_t min_ts_;
  uint64_t max_ts_;

  std::multimap<GenericKey, std::pair<GenericKey, uint64_t>> map_;

};

class WriteBuffer {

public:
  WriteBuffer() {
    data_ = new char[WRITE_BUFFER_SIZE];
    memset(data_, 0, WRITE_BUFFER_SIZE);
    offset_ = 0;
  }

  ~WriteBuffer() {
    delete[] data_;
    data_ = nullptr;
  }

  void reset() { 
    memset(data_, 0, WRITE_BUFFER_SIZE);
    offset_ = 0;
  }

  char *data_;
  size_t offset_;
};

class FileMetadata {
public:
  FileMetadata() {}

  FileMetadata(const size_t file_size, const uint64_t min_ts, const uint64_t max_ts) {
    file_size_ = file_size;
    min_ts_ = min_ts;
    max_ts_ = max_ts;
  }

  FileMetadata(const FileMetadata &metadata) {
    file_size_ = metadata.file_size_;
    min_ts_ = metadata.min_ts_;
    max_ts_ = metadata.max_ts_;
  }

  size_t file_size_;
  uint64_t min_ts_;
  uint64_t max_ts_;
};


class LSM {
public:
  LSM() {
    imm_table_ = new ImmTable();
    write_buffer_ = new WriteBuffer();

    next_file_id_ = 0;
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

  static void persist_callback(void *data, const char *key, const size_t key_size, const char *payload, const size_t payload_size, const uint64_t timestamp) {
    WriteBuffer *buffer = (WriteBuffer*)data;
    // write key_size
    memcpy(buffer->data_ + buffer->offset_, &key_size, sizeof(key_size));
    buffer->offset_ += sizeof(key_size);
    // write payload_size
    memcpy(buffer->data_ + buffer->offset_, &payload_size, sizeof(payload_size));
    buffer->offset_ += sizeof(payload_size);
    // write key
    memcpy(buffer->data_ + buffer->offset_, key, key_size);
    buffer->offset_ += key_size;
    // write payload
    memcpy(buffer->data_ + buffer->offset_, payload, payload_size);
    buffer->offset_ += payload_size;
    // write timestamp
    memcpy(buffer->data_ + buffer->offset_, &timestamp, sizeof(uint64_t));
    buffer->offset_ += sizeof(uint64_t);

    // if exceeds max write buffer size, then flush.

  }

  void persist_imm_table() {

    size_t file_id = next_file_id_;
    next_file_id_++;

    // serialize imm_table_ data to write_buffer_
    imm_table_->iterate(&LSM::persist_callback, (void*)(write_buffer_));

    // persist write_buffer_ data to disk
    write_sst(file_id, write_buffer_->offset_, write_buffer_->data_);


    FileMetadata metadata(write_buffer_->offset_, imm_table_->get_min_ts(), imm_table_->get_max_ts());
  
    assert(sst_files_.find(file_id) == sst_files_.end());
    sst_files_[file_id] = metadata;
    // sst_files_[file_id].file_size_ = write_buffer_->offset_;

    imm_table_->reset();

    write_buffer_->reset();

  }

  void print_file(const size_t file_id) {
    assert(sst_files_.find(file_id) != sst_files_.end());

    size_t file_size = sst_files_.at(file_id).file_size_;

    char *data = new char[file_size];
    memset(data, 0, file_size);
    read_sst(file_id, file_size, data);

    size_t offset = 0;
    while (offset < file_size) {

      // read key_size
      size_t key_size = 0;
      memcpy(&key_size, data + offset, sizeof(key_size));
      offset += sizeof(key_size);
      // read payload_size
      size_t payload_size = 0;
      memcpy(&payload_size, data + offset, sizeof(payload_size));
      offset += sizeof(payload_size);

      // read key
      char *key_str = new char[key_size];
      memcpy(key_str, data + offset, key_size);
      offset += key_size;
      // read payload
      char *payload_str = new char[payload_size];        
      memcpy(payload_str, data + offset, payload_size);
      offset += payload_size;

      uint64_t timestamp = 0;
      memcpy(&timestamp, data + offset, sizeof(uint64_t));
      offset += sizeof(uint64_t);

      std::cout << "timestamp = " << timestamp << std::endl;

      delete[] key_str;
      key_str = nullptr;
      delete[] payload_str;
      payload_str = nullptr;

    }

    delete[] data;
    data = nullptr;

  }

private:

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

  WriteBuffer *write_buffer_;

  size_t next_file_id_;

  std::unordered_map<size_t, FileMetadata> sst_files_;

};

