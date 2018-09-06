#pragma once

#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <unistd.h>

#include "metadata.h"
// #include "trie.h"

const size_t WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
const size_t MAX_FILE_SIZE = WRITE_BUFFER_SIZE * 2;
const size_t MAX_KEY_SPACE_COUNT = 4;
const size_t MAX_LEVEL_DEPTH = 10;


class LSM {

  typedef Trie ImmTable;

  struct WriteBuffer {

    void reset_offset() { offset_ = 0; }

    char *data_;
    size_t offset_;
  };

public:
  LSM() {
    imm_table_ = new Trie();
    imm_table_size_ = 0;
    next_file_id_ = 0;

    write_buffer_.data_ = new char[MAX_FILE_SIZE];
    write_buffer_.offset_ = 0;
  }

  ~LSM() {
    delete imm_table_;
    imm_table_ = nullptr;

    delete[] write_buffer_.data_;
    write_buffer_.data_ = nullptr;

    for (auto metadata : file_metadata_) {
      delete metadata;
      metadata = nullptr;
    }
  }

  void insert(const Slice &key, const Slice &value) {
    imm_table_->insert(key, value);
    imm_table_size_ += key.size_;
    imm_table_size_ += value.size_;

    if (imm_table_size_ > WRITE_BUFFER_SIZE) {
      persist_mem_table();
      imm_table_->reset();
      imm_table_size_ = 0;
    }
    // try_compaction();
  }

  bool find(const Slice &key, Slice &value) {
    bool rt = imm_table_->find(key, value);
    if (rt == true) { return true; } 

    char* data = new char[MAX_FILE_SIZE];

    for (auto metadata : file_metadata_) {
      size_t file_id = metadata->file_id_;
      size_t file_size = metadata->file_size_;
      read_sst(file_id, file_size, data);

      size_t offset = 0;
      while (offset < file_size) {

        // read key_len
        uint32_t key_len = 0;
        memcpy(&key_len, data + offset, sizeof(key_len));
        offset += sizeof(key_len);

        // read value_len
        uint32_t value_len = 0;
        memcpy(&value_len, data + offset, sizeof(value_len));
        offset += sizeof(value_len);

        // read key
        unsigned char *key_str = new unsigned char[key_len];        
        memcpy(key_str, data + offset, key_len);
        offset += key_len;

        int cmp_val = strcmp((char*)(key.data_), (char*)key_str);
        if (cmp_val == 0) {
          // if key matches, then read value
          unsigned char *value_str = new unsigned char[value_len];        
          memcpy(value_str, data + offset, value_len);
          offset += value_len;

          value.data_ = value_str;
          value.size_ = value_len;

          delete[] key_str;
          key_str = nullptr;

          return true;

        } else if (cmp_val > 0) {
          // in this case, break and start scanning next file.

          offset = 0;

          delete[] key_str;
          key_str = nullptr;
          
          break;

        } else {

          offset += value_len;

          delete[] key_str;
          key_str = nullptr;

        }
      }

      assert(offset == 0 || offset == file_size);
    }

    delete[] data;
    data = nullptr;
  }

private:

  void try_compaction() {
    // metadata_set_.find_compaction_candidate();
  }

  void do_compaction(std::vector<Metadata*> file_metadata) {

    char* data = new char[MAX_FILE_SIZE];

    for (auto metadata : file_metadata_) {

    }

    delete[] data;
    data = nullptr;

  }


  // file id + write buffer + ssd
  static int persist_callback(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    WriteBuffer *buffer = (WriteBuffer*)data;
    Slice *s = (Slice*)value;
    uint32_t value_len = s->size_;
    // write key_len
    memcpy(buffer->data_ + buffer->offset_, &key_len, sizeof(key_len));
    buffer->offset_ += sizeof(key_len);
    // write value_len
    memcpy(buffer->data_ + buffer->offset_, &value_len, sizeof(value_len));
    buffer->offset_ += sizeof(value_len);
    // write key
    memcpy(buffer->data_ + buffer->offset_, key, key_len);
    buffer->offset_ += key_len;
    // write value
    memcpy(buffer->data_ + buffer->offset_, s->data_, value_len);
    buffer->offset_ += value_len;
    return 0;
  }

  void persist_mem_table(const size_t span_len) {

    // Slice root_prefix;
    // imm_table_->get_root_prefix(root_prefix);

    // size_t char_count = imm_table_->get_next_char_count();

    // // size_t part_count = 4;

    // // assert(char_count > part_count);

    // // size_t part_char_count = char_count / part_count;

    // // for (size_t i = 0; i < part_count - 1; ++i) {
    // //   size_t part_char_begin = part_char_count * i;
    // //   size_t part_char_end = part_char_count * (i + 1) - 1;
    // // }



    size_t file_id = next_file_id_;
    next_file_id_++;

    // copy elements in imm_table to write_buffer_
    imm_table_->iterate(&LSM::persist_callback, (void*)(&write_buffer_));
    // if nothing is found, then return.
    // if (write_buffer_.offset_ == 0) { return; }
    // write_sst(file_id, write_buffer_.offset_, write_buffer_.data_);

    // Metadata *metadata = new Metadata(file_id, write_buffer_.offset_, 0);
    // file_metadata_.push_back(metadata);

    // write_buffer_.reset_offset();
  }

  uint64_t write_sst(const size_t file_id, const size_t size, const char *data) {
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

  void print_all_files() const {

    char* data = new char[MAX_FILE_SIZE];

    for (auto metadata : file_metadata_) {
      size_t file_id = metadata->file_id_;
      size_t file_size = metadata->file_size_;
      read_sst(file_id, file_size, data);

      size_t offset = 0;
      while (offset < file_size) {

        // read key_len
        uint32_t key_len = 0;
        memcpy(&key_len, data + offset, sizeof(key_len));
        offset += sizeof(key_len);

        // read value_len
        uint32_t value_len = 0;
        memcpy(&value_len, data + offset, sizeof(value_len));
        offset += sizeof(value_len);

        // read key
        unsigned char *key_str = new unsigned char[key_len];        
        memcpy(key_str, data + offset, key_len);
        offset += key_len;
        
        // read value
        unsigned char *value_str = new unsigned char[value_len];        
        memcpy(value_str, data + offset, value_len);
        offset += value_len;

        delete[] key_str;
        key_str = nullptr;
        delete[] value_str;
        value_str = nullptr;
      }
      assert(offset == file_size);
    }

    delete[] data;
    data = nullptr;
  }


private:
  ImmTable* imm_table_;

  size_t imm_table_size_;

  size_t next_file_id_;

  WriteBuffer write_buffer_;

  std::vector<Metadata*> file_metadata_;

};
