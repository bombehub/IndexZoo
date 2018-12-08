#include <iostream>

#include "fast_random.h"
#include "time_measurer.h"

#include "lsm_trie.h"


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

  LSMTrie storage(key_size, payload_size);
  for (auto &kv : kv_logs) {
    storage.insert(kv.key_, kv.payload_, kv.timestamp_);
  }

  for (auto &kv : kv_logs) {
    std::vector<GenericKey> ret_payloads;
    storage.lookup(kv.key_, kv.timestamp_, ret_payloads);
    assert(ret_payloads.size() == 1);
  }

  storage.persist();

  std::cout << "imm size = " << storage.get_imm_size() << std::endl;

  storage.clear();

  std::cout << "imm size = " << storage.get_imm_size() << std::endl;

  storage.load();

  std::cout << "imm size = " << storage.get_imm_size() << std::endl;

}

void mytest() {

  char c0 = 0xff;
  char c1 = 0x7f;

  std::cout << (c0 < c1) << std::endl;

  uint8_t d0 = (uint8_t)c0;
  uint8_t d1 = (uint8_t)c1;

  std::cout << (d0 < d1) << std::endl;
  std::cout << unsigned(d0) << " " << unsigned(d1) << std::endl;
}

int main(int argc, char* argv[]) {
  size_t key_count = 1000;
  test(key_count);
}