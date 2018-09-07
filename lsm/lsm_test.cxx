#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fast_random.h"
#include "time_measurer.h"

#include "generic_key.h"
#include "generic_data_table.h"

#include "index_all.h"

#include "new_lsm.h"

struct DataEntry {
  DataEntry(const std::string &key, 
            const std::string &payload, 
            const uint64_t timestamp) :
    key_(key.data(), key.size()), 
    payload_(payload.data(), payload.size()), 
    timestamp_(timestamp) { }

  GenericKey key_;
  GenericKey payload_;
  uint64_t timestamp_;
};

void test(const size_t key_count) {
  
  FastRandom rand;

  size_t key_size = 8;
  size_t payload_size = 100;

  LSM storage;

  std::vector<DataEntry> kv_logs;
  uint64_t ts_counter = 0;
  for (size_t i = 0; i < key_count; ++i) {
    std::string key;
    rand.next_readable_string(key_size, key);
    std::string payload;
    rand.next_readable_string(payload_size, payload);
    uint64_t timestamp = ts_counter;

    kv_logs.push_back(DataEntry(key, payload, timestamp));

    ++ts_counter;
  }

  for (auto kv : kv_logs) {
    storage.insert(kv.key_, kv.payload_, kv.timestamp_);
  }

  size_t key_found = 0;
  for (auto kv : kv_logs) {
    GenericKey ret_payload;
    bool rt = storage.find(kv.key_, MAX_TS, ret_payload);
    if (rt == true) {
      ++key_found;
    }
  }
  std::cout << "key found = " << key_found << std::endl;

  storage.persist_imm_table();
  storage.print_file(0);

}

int main(int argc, char* argv[]) {

  size_t key_count = 100;
  if (argc == 2) {
    key_count = atoi(argv[1]);
  }
  std::cout << "key count = " << key_count << std::endl;

  test(key_count);
}
