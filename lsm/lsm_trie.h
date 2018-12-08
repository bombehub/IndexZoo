#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <unistd.h>

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

  void populate(std::multimap<GenericKey, std::pair<GenericKey, uint64_t>>& imm_buffer) {
    for (auto& entry : entries_) {
      imm_buffer.insert( {entry.key_, {entry.payload_, entry.timestamp_}} );
    }
  }

  void persist() {
    size_t entry_size = key_size_ + payload_size_ + sizeof(uint64_t);
    size_t total_size = entry_size * entries_.size() + sizeof(size_t);
    
    char *mem_buffer = new char[total_size];
    
    size_t offset = 0;
    size_t entry_count = entries_.size();
    memcpy(mem_buffer, &entry_count, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    for (auto& entry : entries_) {
      memcpy(mem_buffer + offset, entry.key_.raw(), key_size_);
      offset += key_size_;
      memcpy(mem_buffer + offset, &entry.timestamp_, sizeof(uint64_t));
      offset += sizeof(uint64_t);
      memcpy(mem_buffer + offset, entry.payload_.raw(), payload_size_);
      offset += payload_size_;
    }

    assert(offset == total_size);

    FILE* file = open_write_file(sst_id_);
    write_file(file, total_size, mem_buffer);
    close_file(file);

    delete[] mem_buffer;
    mem_buffer = nullptr;
  }

  void load() {

    size_t entry_count;

    FILE* file = open_read_file(sst_id_);
    read_file(file, sizeof(size_t), (char*)(&entry_count));

    size_t entry_size = key_size_ + payload_size_ + sizeof(uint64_t);
    size_t total_size = entry_size * entry_count;
    
    char *mem_buffer = new char[total_size];
    read_file(file, total_size, mem_buffer);

    char *key_str = new char[key_size_];
    char *payload_str = new char[payload_size_];
    uint64_t timestamp;

    size_t offset = 0;
    
    while (offset < total_size) {
      memcpy(mem_buffer + offset, key_str, key_size_);
      offset += key_size_;
      memcpy(mem_buffer + offset, payload_str, payload_size_);
      offset += payload_size_;
      memcpy(mem_buffer + offset, &timestamp, sizeof(uint64_t));
      offset += sizeof(uint64_t);
      entries_.push_back( {GenericKey(key_str, key_size_), GenericKey(payload_str, payload_size_), timestamp} );
    }

    close_file(file);


    delete[] key_str;
    key_str = nullptr;

    delete[] payload_str;
    payload_str = nullptr;

    delete[] mem_buffer;
    mem_buffer = nullptr;
  }

  size_t size() const { return entries_.size(); }

  size_t get_id() const { return sst_id_; }

private:

  FILE* open_write_file(const size_t file_id) {
    std::string filename = get_filename(file_id);
    FILE *file = fopen(filename.c_str(), "w+b");
    fseek(file, 0, SEEK_SET);
    return file;
  }

  FILE* open_read_file(const size_t file_id) {
    std::string filename = get_filename(file_id);
    FILE *file = fopen(filename.c_str(), "r+b");
    fseek(file, 0, SEEK_SET);
    return file;
  }

  void close_file(FILE* file) {
    fclose(file);
  }

  void write_file(FILE* file, const size_t size, const char *data) {
    size_t ret = fwrite(data, size, 1, file);
    assert(ret == 1);
    fflush(file);
    fsync(fileno(file));
  }

  void read_file(FILE* file, size_t size, char *data) const {
    int ret = fread(data, size, 1, file);
    assert(ret == 1);
  }

  std::string get_filename(const size_t file_id) const {
    return std::string("/tmp/sst" + std::to_string(file_id) + ".dat");
  }

private:
  std::vector<Entry> entries_;

  size_t key_size_;
  size_t payload_size_;
  size_t sst_id_;
};

class FileMetadata {

struct FileNode {

  std::vector<FileNode*> children_;
  std::vector<size_t> file_ids_;
};


public:
  FileMetadata() {}
  // : root_(nullptr) {}
  ~FileMetadata() {
    // delete root_;
    // root_ = nullptr;
  }

  void add(const size_t branch, const size_t file_id) {
    
    file_id_map_[branch].push_back(file_id);

    // if (root_ == nullptr) {
    //   root_ = new FileNode();
    // }
    // FileNode* node = 
  }

  std::vector<size_t> get(const size_t branch) {

    return file_id_map_[branch];

  }

private:
  
  std::unordered_map<int, std::vector<size_t>> file_id_map_;
  
  // FileNode *root_;

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
      ssts[i]->persist();
      file_metadata_.add(i, ssts[i]->get_id());

      delete ssts[i];
      ssts[i] = nullptr;
    }

    delete[] ssts;
    ssts = nullptr;
  }

  void load() {

    SST **ssts = new SST*[fanout_];

    for (size_t i = 0; i < fanout_; ++i) {
      std::vector<size_t> ret = file_metadata_.get(i);
      
      ssts[i] = new SST(key_size_, payload_size_, file_metadata_.get(i)[0]);
      ssts[i]->load();

      ssts[i]->populate(imm_buffer_);
    }

    for (size_t i = 0; i < fanout_; ++i) {
      delete ssts[i];
      ssts[i] = nullptr;
    }

    delete[] ssts;
    ssts = nullptr;

  }

  size_t get_imm_size() {
    return imm_buffer_.size();
  }

  void clear() {
    imm_buffer_.clear();
  }

private:
  std::multimap<GenericKey, std::pair<GenericKey, uint64_t>> imm_buffer_;

  FileMetadata file_metadata_;
  
  size_t key_size_;
  size_t payload_size_;
  size_t sst_id_;

  size_t fanout_;
};
