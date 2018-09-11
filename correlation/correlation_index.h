#pragma once

#include <vector>
#include <map>


class CorrelationIndex {

  struct AttributePair {
    AttributePair() : guest_(0), host_(0) {}
    AttributePair(const uint64_t target, const uint64_t host) : guest_(target), host_(host) {}

    uint64_t guest_;
    uint64_t host_;
  };

  static bool compare_func(AttributePair &lhs, AttributePair &rhs) {
    return lhs.guest_ < rhs.guest_;
  }

  struct Stats {

    Stats() : 
      is_first_match_(true), 
      find_op_count_(0), 
      find_op_profile_count_(0), 
      find_op_guess_distance_(0) {}

    void increment_find_op_counter() {
      find_op_count_++;
      is_first_match_ = true;
    }

    void measure_find_op_guess_distance(const int64_t guess_pos, const int64_t find_pos) {
      // record guess distance
      if (find_op_count_ % 1000 == 0) {
        if (is_first_match_ == true) {
          find_op_guess_distance_ += std::abs(guess_pos - find_pos);
          find_op_profile_count_ += 1;
          is_first_match_ = false;
        }
      }
    }

    bool is_first_match_;
    uint64_t find_op_count_;
    uint64_t find_op_profile_count_;
    uint64_t find_op_guess_distance_;
  };

public:
  CorrelationIndex(const size_t num_segments = 1) {

    ASSERT(num_segments >= 1, "must have at least one segment");

    num_segments_ = num_segments;
    segment_key_boundaries_ = new uint64_t[num_segments_ + 1];
    memset(segment_key_boundaries_, 0, sizeof(uint64_t) * (num_segments_ + 1));

    segment_offset_boundaries_ = new size_t[num_segments_];
    memset(segment_offset_boundaries_, 0, sizeof(size_t) * num_segments_);

    segment_sizes_ = new size_t[num_segments_];
    memset(segment_sizes_, 0, sizeof(size_t) * num_segments_);

  }

  ~CorrelationIndex() {

    delete[] container_;
    container_ = nullptr;

    delete[] segment_key_boundaries_;
    segment_key_boundaries_ = nullptr;

    delete[] segment_offset_boundaries_;
    segment_offset_boundaries_ = nullptr;

    delete[] segment_sizes_;
    segment_sizes_ = nullptr;

  }

  void lookup(const uint64_t key, std::vector<uint64_t> &ret_keys) {

    stats_.increment_find_op_counter();

    if (this->size_ == 0) {
      return;
    }

    if (key > key_max_ || key < key_min_) {
      return;
    }

    // all keys are equal
    if (key_min_ == key_max_) {
      if (key_min_ == key) {
        for (size_t i = 0; i < this->size_; ++i) {
          ret_keys.push_back(this->container_[i].host_);
        }
      }
      return;
    }

    // find suitable segment
    size_t segment_id = (key - key_min_) / ((key_max_ - key_min_) / num_segments_);
    if (segment_id > num_segments_ - 1) {
      segment_id = num_segments_ - 1;
    }

    // the key should fall into: 
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ) -- if 0 <= i < num_segments_ - 1
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ] -- if i == num_segments_ - 1
    if (segment_id < num_segments_ - 1) {

      ASSERT(segment_key_boundaries_[segment_id] <= key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << key);
      ASSERT(key < segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << key << " " << segment_key_boundaries_[segment_id + 1]);

    } else {

      ASSERT(segment_id == num_segments_ - 1, 
        "incorrect segment id: " << segment_id << " " << num_segments_ - 1);

      ASSERT(segment_key_boundaries_[segment_id] <= key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << key);
      ASSERT(key <= segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << key << " " << segment_key_boundaries_[segment_id + 1]);
    }

    uint64_t segment_key_range = segment_key_boundaries_[segment_id + 1] - segment_key_boundaries_[segment_id];
    
    // guess where the data lives
    int64_t guess = int64_t((key - segment_key_boundaries_[segment_id]) * 1.0 / segment_key_range * (segment_sizes_[segment_id] - 1) + segment_offset_boundaries_[segment_id]);

    // TODO: workaround!!
    if (guess >= this->size_) {
      guess = this->size_ - 1;
    }

    int64_t origin_guess = guess;
    
    // if the guess is correct
    if (this->container_[guess].guest_ == key) {

      stats_.measure_find_op_guess_distance(origin_guess, guess);

      ret_keys.push_back(this->container_[guess].host_);
      
      // move left
      int64_t guess_lhs = guess - 1;
      while (guess_lhs >= 0) {

        if (this->container_[guess_lhs].guest_ == key) {
          ret_keys.push_back(this->container_[guess_lhs].host_);
          guess_lhs -= 1;
        } else {
          break;
        }
      }
      // move right
      int64_t guess_rhs = guess + 1;
      while (guess_rhs <= this->size_ - 1) {

        if (this->container_[guess_rhs].guest_ == key) {
          ret_keys.push_back(this->container_[guess_rhs].host_);
          guess_rhs += 1;
        } else {
          break;
        }
      }
    }
    // if the guess is larger than the key
    else if (this->container_[guess].guest_ > key) {
      // move left
      guess -= 1;
      while (guess >= 0) {

        if (this->container_[guess].guest_ < key) {
          break;
        }
        else if (this->container_[guess].guest_ > key) {
          guess -= 1;
          continue;
        } 
        else {

          stats_.measure_find_op_guess_distance(origin_guess, guess);

          ret_keys.push_back(this->container_[guess].host_);
          guess -= 1;
          continue;
        }
      }
    } 
    // if the guess is smaller than the key
    else {
      // move right
      guess += 1;
      while (guess < this->size_ - 1) {

        if (this->container_[guess].guest_ > key) {
          break;
        }
        else if (this->container_[guess].guest_ < key) {
          guess += 1;
          continue;
        }
        else {
          
          stats_.measure_find_op_guess_distance(origin_guess, guess);

          ret_keys.push_back(this->container_[guess].host_);
          guess += 1;
          continue;
        }
      }
    }
    return;
  }

  void lookup_range(const uint64_t &lhs_key, const uint64_t &rhs_key, std::vector<uint64_t> &ret_keys) {

    if (lhs_key > rhs_key) { return; }

    if (lhs_key == rhs_key) {
      lookup(lhs_key, ret_keys);
      return;
    }

    if (this->size_ == 0) {
      return;
    }

    if (lhs_key > key_max_ || rhs_key < key_min_) {
      return;
    }

    // all keys are equal
    if (key_min_ == key_max_) {
      if (key_min_ >= lhs_key && key_min_ <= rhs_key) {
        for (size_t i = 0; i < this->size_; ++i) {
          ret_keys.push_back(this->container_[i].host_);
        }
      }
      return;
    }

    int64_t lower_bound = find_lower_bound(lhs_key);
    int64_t upper_bound = find_upper_bound(rhs_key);

    for (size_t i = lower_bound; i <= upper_bound; ++i) {
      ret_keys.push_back(this->container_[i].host_);
    }
    return;
  }


  void construct(const std::vector<uint64_t> &guest_column, const std::vector<uint64_t> &host_column) {

    ASSERT(guest_column.size() == host_column.size(), "guest and host columns must have same cardinality");

    size_ = guest_column.size();

    container_ = new AttributePair[size_];

    for (size_t i = 0; i < guest_column.size(); ++i) {
      container_[i].guest_ = guest_column.at(i);
      container_[i].host_ = host_column.at(i);
    }

    // sort data
    std::sort(container_, container_ + size_, compare_func);

    key_min_ = this->container_[0].guest_; // min value
    key_max_ = this->container_[this->size_ - 1].guest_; // max value

    uint64_t key_range = key_max_ - key_min_;
    // key range size for each segment
    uint64_t segment_key_range = key_range / num_segments_;

    // assign key boundaries
    segment_key_boundaries_[0] = key_min_;
    segment_key_boundaries_[num_segments_] = key_max_;
    for (size_t i = 1; i < num_segments_; ++i) {
      segment_key_boundaries_[i] = this->container_[0].guest_ + segment_key_range * i;
    }

    size_t current_offset = 0;

    segment_offset_boundaries_[0] = current_offset;

    for (size_t i = 0; i < num_segments_ - 1; ++i) {
      // scan the entire table to find offset boundaries
      while (this->container_[current_offset].guest_ < segment_key_boundaries_[i + 1]) {
        ++segment_sizes_[i];
        ++current_offset;
      }
      segment_offset_boundaries_[i + 1] = current_offset;
    }

    segment_sizes_[num_segments_ - 1] = this->size_ - current_offset;

  }

  void print() const {

    std::cout << "aggregated guess distance = " << stats_.find_op_guess_distance_ << std::endl;

    std::cout << "number of profiled find operations = " << stats_.find_op_profile_count_ << std::endl;

    std::cout << "average guess distance = " << stats_.find_op_guess_distance_ * 1.0 / stats_.find_op_profile_count_ << std::endl;
  }

private:

  int64_t find_lower_bound(const uint64_t &lower_key) {

    ASSERT(lower_key <= key_max_, "lower_key must be <= key_max_");

    if (lower_key <= key_min_) {
      return 0;
    }

    size_t segment_id = (lower_key - key_min_) / ((key_max_ - key_min_) / num_segments_);
    if (segment_id > num_segments_ - 1) {
      segment_id = num_segments_ - 1;
    }

    // the lower_key should fall into: 
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ) -- if 0 <= i < num_segments_ - 1
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ] -- if i == num_segments_ - 1
    if (segment_id < num_segments_ - 1) {

      ASSERT(segment_key_boundaries_[segment_id] <= lower_key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << lower_key);
      ASSERT(lower_key < segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << lower_key << " " << segment_key_boundaries_[segment_id + 1]);

    } else {

      ASSERT(segment_id == num_segments_ - 1, 
        "incorrect segment id: " << segment_id << " " << num_segments_ - 1);

      ASSERT(segment_key_boundaries_[segment_id] <= lower_key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << lower_key);
      ASSERT(lower_key <= segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << lower_key << " " << segment_key_boundaries_[segment_id + 1]);
    }

    uint64_t segment_key_range = segment_key_boundaries_[segment_id + 1] - segment_key_boundaries_[segment_id];
    // guess where the data lives
    int64_t guess = int64_t((lower_key - segment_key_boundaries_[segment_id]) * 1.0 / segment_key_range * (segment_sizes_[segment_id] - 1) + segment_offset_boundaries_[segment_id]);

    // TODO: workaround!!
    if (guess >= this->size_) {
      guess = this->size_ - 1;
    }

    if (this->container_[guess].guest_ >= lower_key) {
      // move left
      while (guess - 1 >= 0) {
        if (this->container_[guess - 1].guest_ >= lower_key) {
          --guess;
        } else {
          return guess;
        }
      }
      return guess;

    } else {
      // move right
      ++guess;
      while (guess < this->size_) {
        if (this->container_[guess].guest_ < lower_key) {
          ++guess;
        } else {
          return guess;
        }
      }
      ASSERT(false, "shouldn't touch this line of code");
      return guess;
    }

  }
  
  int64_t find_upper_bound(const uint64_t &upper_key) {
    
    ASSERT(upper_key >= key_min_, "upper_key must be >= key_min_");

    if (upper_key >= key_max_) {
      return this->size_ - 1;
    }

    size_t segment_id = (upper_key - key_min_) / ((key_max_ - key_min_) / num_segments_);
    if (segment_id > num_segments_ - 1) {
      segment_id = num_segments_ - 1;
    }

    // the upper_key should fall into: 
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ) -- if 0 <= i < num_segments_ - 1
    //  [ segment_key_boundaries_[i], segment_key_boundaries_[i + 1] ] -- if i == num_segments_ - 1
    if (segment_id < num_segments_ - 1) {

      ASSERT(segment_key_boundaries_[segment_id] <= upper_key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << upper_key);
      ASSERT(upper_key < segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << upper_key << " " << segment_key_boundaries_[segment_id + 1]);

    } else {

      ASSERT(segment_id == num_segments_ - 1, 
        "incorrect segment id: " << segment_id << " " << num_segments_ - 1);

      ASSERT(segment_key_boundaries_[segment_id] <= upper_key, 
        "beyond boundary: " << segment_key_boundaries_[segment_id] << " " << upper_key);
      ASSERT(upper_key <= segment_key_boundaries_[segment_id + 1], 
        "beyond boundary: " << upper_key << " " << segment_key_boundaries_[segment_id + 1]);
    }

    uint64_t segment_key_range = segment_key_boundaries_[segment_id + 1] - segment_key_boundaries_[segment_id];
    // guess where the data lives
    int64_t guess = int64_t((upper_key - segment_key_boundaries_[segment_id]) * 1.0 / segment_key_range * (segment_sizes_[segment_id] - 1) + segment_offset_boundaries_[segment_id]);

    // TODO: workaround!!
    if (guess >= this->size_) {
      guess = this->size_ - 1;
    }

    if (this->container_[guess].guest_ <= upper_key) {
      // move right
      while (guess +1 <= this->size_ - 1) {
        if (this->container_[guess + 1].guest_ <= upper_key) {
          ++guess;
        } else {
          return guess;
        }
      }
      return guess;

    } else {
      // move left
      --guess;
      while (guess > 0) {
        if (this->container_[guess].guest_ > upper_key) {
          --guess;
        } else {
          return guess;
        }
      }
      ASSERT(false, "shouldn't touch this line of code");
      return guess;
    }
  }


private:

  AttributePair *container_;
  size_t size_;

  size_t num_segments_;

  uint64_t key_min_;
  uint64_t key_max_;

  // there are num_segments_ + 1 key boundaries in total
  uint64_t *segment_key_boundaries_; 

  // there are num_segments_ offset boundaries in total
  size_t *segment_offset_boundaries_;

  // there are num_segments_ elements in segment_sizes_
  size_t *segment_sizes_;

  Stats stats_;
};
