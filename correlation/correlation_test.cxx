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

void primary_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BaseGenericIndex *primary_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  FastRandom rand_gen;
  GenericKey query_key(sizeof(uint64_t));
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);
    memcpy(query_key.raw(), (char*)(&key), sizeof(uint64_t));

    std::vector<Uint64> offsets;

    primary_index->find(query_key, offsets);

    for (auto offset : offsets) {
      char *value = data_table->get_tuple(offset);
      size_t attr1_offset = tuple_schema.get_attr_offset(1);
      size_t attr2_offset = tuple_schema.get_attr_offset(2);
      uint64_t attr1_ret = *(uint64_t*)(value + attr1_offset);
      uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);
      std::cout << attr1_ret << " " << attr2_ret << std::endl;
    }
    std::cout << "===========" << std::endl;
  }

}

void secondary_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BaseGenericIndex *primary_index, stx::btree_multimap<uint64_t, uint64_t> *secondary_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  FastRandom rand_gen;
  GenericKey query_key(sizeof(uint64_t));
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);
    auto ret = secondary_index->equal_range(key);
    // found primary keys
    for (auto iter = ret.first; iter != ret.second; ++iter) {
      uint64_t pkey = iter->second;
      memcpy(query_key.raw(), (char*)(&pkey), sizeof(uint64_t));

      std::vector<Uint64> offsets;

      primary_index->find(query_key, offsets);

      for (auto offset : offsets) {
        char *value = data_table->get_tuple(offset);
        size_t attr1_offset = tuple_schema.get_attr_offset(1);
        size_t attr2_offset = tuple_schema.get_attr_offset(2);
        uint64_t attr1_ret = *(uint64_t*)(value + attr1_offset);
        uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);
        std::cout << attr1_ret << " " << attr2_ret << std::endl;
      }
      std::cout << "===========" << std::endl;

    }

  }

}


class CorrelationIndex {
public:
  CorrelationIndex() {}
  ~CorrelationIndex() {}

  void construct(std::vector<uint64_t> &target_column, std::vector<uint64_t> &host_column) {
    assert(target_column.size() == host_column.size());

    for (size_t i = 0; i < target_column.size(); ++i) {
      corr_index_.insert( { target_column.at(i), host_column.at(i) } );
    }
  }

  void lookup(const uint64_t key, std::vector<uint64_t> &ret_keys) {
    auto ret = corr_index_.equal_range(key);
    for (auto it = ret.first; it != ret.second; ++it) {
      ret_keys.push_back(it->second);
    }
  }

private:
  std::multimap<uint64_t, uint64_t> corr_index_;

};

void correlation_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BaseGenericIndex *primary_index, stx::btree_multimap<uint64_t, uint64_t> *secondary_index, CorrelationIndex *correlation_index, const std::vector<uint64_t> &keys) {

  size_t tuple_count = keys.size();
  size_t query_count = tuple_count;

  FastRandom rand_gen;
  GenericKey query_key(sizeof(uint64_t));
  for (size_t query_id = 0; query_id < query_count; ++query_id) {

    uint64_t key = keys.at(rand_gen.next<uint64_t>() % tuple_count);
    std::vector<uint64_t> host_keys;
    // found host keys
    correlation_index->lookup(key, host_keys);
    for (auto host_key : host_keys) {
      auto ret = secondary_index->equal_range(host_key);
      // found primary keys
      for (auto iter = ret.first; iter != ret.second; ++iter) {
        uint64_t pkey = iter->second;
        memcpy(query_key.raw(), (char*)(&pkey), sizeof(uint64_t));

        std::vector<Uint64> offsets;

        primary_index->find(query_key, offsets);

        for (auto offset : offsets) {
          char *value = data_table->get_tuple(offset);
          size_t attr1_offset = tuple_schema.get_attr_offset(1);
          size_t attr2_offset = tuple_schema.get_attr_offset(2);
          uint64_t attr1_ret = *(uint64_t*)(value + attr1_offset);
          uint64_t attr2_ret = *(uint64_t*)(value + attr2_offset);
          std::cout << attr1_ret << " " << attr2_ret << std::endl;
        }
        std::cout << "===========" << std::endl;
      }
    }
  }
}

void test1() {

  TupleSchema tuple_schema;
  // add five uint64_t attributes
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));
  tuple_schema.add_attr(sizeof(uint64_t));

  IndexType index_type = IndexType::D_ST_StxBtree;

  size_t key_size = sizeof(uint64_t);
  size_t value_size = sizeof(uint64_t) * 3;
  std::unique_ptr<GenericDataTable> data_table(
    new GenericDataTable(key_size, value_size));
  // primary index
  std::unique_ptr<BaseGenericIndex> primary_index(
    create_generic_index(index_type, data_table.get()));

  primary_index->prepare_threads(1);
  primary_index->register_thread(0);

  stx::btree_multimap<uint64_t, uint64_t> sindex0;
  stx::btree_multimap<uint64_t, uint64_t> sindex1;
  CorrelationIndex correlation_index;

  FastRandom rand_gen;

  size_t tuple_count = 1000;

  GenericKey tuple_key(key_size);
  GenericKey tuple_value(value_size);

  std::vector<uint64_t> attr0s;
  std::vector<uint64_t> attr1s;
  std::vector<uint64_t> attr2s;

  for (size_t tuple_id = 0; tuple_id < tuple_count; ++tuple_id) {
    uint64_t attr0 = rand_gen.next<uint64_t>(); // primary key
    uint64_t attr1 = tuple_id * tuple_id;
    uint64_t attr2 = tuple_id;
    uint64_t attr3 = rand_gen.next<uint64_t>();

    attr0s.push_back(attr0);
    attr1s.push_back(attr1);
    attr2s.push_back(attr2);

    memcpy(tuple_key.raw(), (char*)(&attr0), sizeof(uint64_t));

    memcpy(tuple_value.raw(), (char*)(&attr1), sizeof(uint64_t));
    memcpy(tuple_value.raw() + sizeof(uint64_t), (char*)(&attr2), sizeof(uint64_t));
    memcpy(tuple_value.raw() + sizeof(uint64_t) * 2, (char*)(&attr3), sizeof(uint64_t));

    // insert into table
    OffsetT offset = data_table->insert_tuple(tuple_key.raw(), tuple_key.size(), tuple_value.raw(), tuple_value.size());

    // update primary index
    primary_index->insert(tuple_key, offset.raw_data());

    // update secondary indexes
    sindex0.insert( {attr1, attr0} );
    sindex1.insert( {attr2, attr0} );

  }

  correlation_index.construct(attr2s, attr1s);

  // primary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), attr0s);

  // secondary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), &sindex0, attr1s);

  correlation_index_lookup(tuple_schema, data_table.get(), primary_index.get(), &sindex0, &correlation_index, attr2s);
}


int main() {
  test1();
}
