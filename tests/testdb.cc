//
// Created by tramboo on 2019/12/24.
//
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"

using namespace std;
using namespace rocksdb;

std::string kDBPath = "../data/db_test";

int main() {
    DB* db;
    Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    // Enable prefix bloom for mem tables
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));

    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options = BlockBasedTableOptions();
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    s = db->Put(WriteOptions(), "key1", "value1");
    assert(s.ok());
    s = db->Put(WriteOptions(), "key2", "value2");
    assert(s.ok());
    s = db->Put(WriteOptions(), "key4", "value3");
    assert(s.ok());
    s = db->Put(WriteOptions(), "key5", "value4");
    assert(s.ok());
    s = db->Put(WriteOptions(), "otherPrefix1", "otherValue1");
    assert(s.ok());
    s = db->Put(WriteOptions(), "abc", "abcvalue1");
    assert(s.ok());

    auto iter = db->NewIterator(ReadOptions());
    Slice prefix = options.prefix_extractor->Transform("key3");
    for (iter->Seek("key3"); iter->Valid() && iter->key().starts_with(prefix); iter->Next())
    {
        std::cout << iter->key().ToString() << ": " << iter->value().ToString() << std::endl;
    }

    db->Delete(WriteOptions(), "key3");
    delete db;
    return 0;
}