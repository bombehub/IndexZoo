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

#include "correlation_index.h"
#include "tuple_schema.h"


typedef stx::btree_multimap<uint64_t, uint64_t> SecondaryIndex;

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

void secondary_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BaseGenericIndex *primary_index, SecondaryIndex *secondary_index, const std::vector<uint64_t> &keys) {

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

void correlation_index_lookup(TupleSchema &tuple_schema, GenericDataTable *data_table, BaseGenericIndex *primary_index, SecondaryIndex *secondary_index, BaseCorrelationIndex *correlation_index, const std::vector<uint64_t> &keys) {

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
          if (attr2_ret == key) {
            std::cout << attr1_ret << " " << attr2_ret << std::endl;
          }
        }
        std::cout << "===========" << std::endl;
      }
    }
  }
}

void test() {

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

  std::unique_ptr<SecondaryIndex> secondary_index(new SecondaryIndex());
  std::unique_ptr<BaseCorrelationIndex> correlation_index(
    new FullCorrelationIndex());

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
    secondary_index->insert( {attr1, attr0} );

  }

  correlation_index->construct(attr2s, attr1s);

  // primary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), attr0s);

  // secondary_index_lookup(tuple_schema, data_table.get(), primary_index.get(), secondary_index.get(), attr1s);

  correlation_index_lookup(tuple_schema, data_table.get(), primary_index.get(), secondary_index.get(), correlation_index.get(), attr2s);
}


int main() {
  test();
}
