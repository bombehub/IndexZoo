#pragma once

#include <cstdlib>
#include <string>
#include <cstring>
#include <iostream>

const size_t MAX_ATTR_COUNT = 20;

struct TupleSchema {

public:
  TupleSchema() {
    memset(attr_widths_, 0, sizeof(size_t) * MAX_ATTR_COUNT);
    memset(attr_offsets_, 0, sizeof(size_t) * MAX_ATTR_COUNT);
    attr_count_ = 0;
  }

  void add_attr(const size_t width) {
    attr_widths_[attr_count_] = width;
    if (attr_count_ == 0) {
      attr_offsets_[attr_count_] = 0;
    } else {
      attr_offsets_[attr_count_] = attr_offsets_[attr_count_ - 1] + attr_widths_[attr_count_ - 1];
    }
    attr_count_ += 1;
  }

  size_t get_attr_width(const size_t attr_id) const {
    if (attr_id < attr_count_) {
      return attr_widths_[attr_id];
    } else {
      return 0;
    }
  }

  size_t get_attr_offset(const size_t attr_id) const {
    if (attr_id < attr_count_) {
      return attr_offsets_[attr_id];
    } else {
      return 0;
    }
  }

  void print() const {
    for (size_t i = 0; i < attr_count_; ++i) {
      std::cout << attr_offsets_[i] << " " << attr_widths_[i] << std::endl;
    }
  }


private:
  size_t attr_widths_[MAX_ATTR_COUNT];
  size_t attr_offsets_[MAX_ATTR_COUNT];
  size_t attr_count_;
};