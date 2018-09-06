#pragma once

#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>
#include <cstdlib>

#include "common.h"
#include "art.h"


class Trie {

public:
  Trie() {
    art_tree_init(&tree_);
  }
  ~Trie() {
    art_tree_destroy(&tree_);
  }

  void reset() {
    art_tree_destroy(&tree_);
    art_tree_init(&tree_);
  }

  void insert(const Slice &key, const Slice &value) {
    Slice *val = new Slice(value);
    art_insert(&tree_, key.data_, key.size_, (void*)val);
  }

  bool find(const Slice &key, Slice &value) {
    Slice *t = (Slice*)art_search(&tree_, key.data_, key.size_);
    if (t == nullptr) {
      return false;
    } else {
      value = *t;
      return true;
    }
  }

  void iterate(art_callback cb, void *data) {
    art_iter(&tree_, cb, data);
  }

  void iterate_prefix(art_callback cb, void *data) {
    art_iter_prefix(&tree_, cb, data);
  }

  static int count_callback(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    size_t& count = *(size_t*)data;
    count++;
    return 0;
  }

  size_t count() {
    size_t count = 0;
    art_iter(&tree_, &Trie::count_callback, (void*)(&count));
    return count;
  }

  size_t count_prefix(const unsigned char *prefix, const size_t &prefix_length) {
    size_t count = 0;
    art_iter_prefix(&tree_, prefix, prefix_length, &Trie::count_callback, (void*)(&count));
    return count;
  }

  void get_root_prefix() {

  }

  // size_t get_root_element_count() {
  //   return art_root_element_count(&tree_);
  // }


private:
  art_tree tree_;

};

