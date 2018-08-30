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


// let us assume that Table T has five attrs: A, B, C, D, E.
// A frequent query looks like: SELECT D, E where A = ? AND B > ? AND C like '%?'
// Conventional scheme builds index on top of all the three attrs: A, B, and C.
// The problem is, whether we can exploit correlation informations among A, B, and C
// to build more succinct index?

const size_t MAX_ATTR_COUNT = 10;

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

void test() {

  TupleSchema tuple_schema;
  // add five uint64_t attributes
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));


  IndexType index_type = IndexType::D_ST_StxBtree;

  size_t key_size = sizeof(uint64_t) * 3;
  size_t value_size = sizeof(uint64_t) * 2;
  std::unique_ptr<GenericDataTable> data_table(
    new GenericDataTable(key_size, value_size));
  std::unique_ptr<BaseGenericIndex> data_index(
    create_generic_index(index_type, data_table.get()));

  data_index->prepare_threads(1);
  data_index->register_thread(0);

  FastRandom rand_gen;

  size_t max_key = 100;

  size_t tuple_count = 1000;

  GenericKey tuple_key(key_size);
  GenericKey tuple_value(value_size);

  for (size_t tuple_id = 0; tuple_id < tuple_count; ++tuple_id) {
    uint64_t attr0 = rand_gen.next<uint64_t>() % max_key;
    // uint64_t attr1 = rand_gen.next<uint64_t>() % max_key;
    // uint64_t attr2 = rand_gen.next<uint64_t>() % max_key;
    uint64_t attr1 = attr0;
    uint64_t attr2 = attr0;
    uint64_t attr3 = rand_gen.next<uint64_t>();
    uint64_t attr4 = rand_gen.next<uint64_t>();

    memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));
    memcpy(tuple_key.raw() + sizeof(uint64_t), (char*)(&attr1), sizeof(uint64_t));
    memcpy(tuple_key.raw() + sizeof(uint64_t) * 2, (char*)(&attr2), sizeof(uint64_t));

    memcpy(tuple_value.raw(), (char*)(&attr3), sizeof(uint64_t));
    memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr4), sizeof(uint64_t));

    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    data_index->insert(tuple_key, offset.raw_data());
  }

  // tuple_schema.print();

  size_t query_count = 1000;

  GenericKey query_key(key_size);

  for (size_t query_id = 0; query_id < query_count; ++query_id) {
    uint64_t attr0 = rand_gen.next<uint64_t>() % max_key;
    // uint64_t attr1 = rand_gen.next<uint64_t>() % max_key;
    // uint64_t attr2 = rand_gen.next<uint64_t>() % max_key;
    uint64_t attr1 = attr0;
    uint64_t attr2 = attr0;

    memcpy(query_key.raw(), (char*)(&attr0), sizeof(uint64_t));
    memcpy(query_key.raw() + sizeof(uint64_t), (char*)(&attr1), sizeof(uint64_t));
    memcpy(query_key.raw() + sizeof(uint64_t) * 2, (char*)(&attr2), sizeof(uint64_t));

    std::vector<Uint64> offsets;

    data_index->find(query_key, offsets);

    std::cout << "========result========" << std::endl;
    for (auto offset : offsets) {
      char *value = data_table->get_tuple(offset);
      size_t attr3_offset = tuple_schema.get_attr_offset(3);
      size_t attr4_offset = tuple_schema.get_attr_offset(4);
      uint64_t attr3_ret = *(uint64_t*)(value + attr3_offset);
      uint64_t attr4_ret = *(uint64_t*)(value + attr4_offset);
      std::cout << attr3_ret << " " << attr4_ret << std::endl;
    }
    std::cout << "======================" << std::endl;

  }


}


int main() {
  test();
}