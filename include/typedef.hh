//
// Created by tramboo on 2020/3/17.
//

#include <cstdint>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

namespace lightgraph
{

// Options in rocksdb
typedef rocksdb::Status LStatus;
typedef rocksdb::Options LOptions;
typedef rocksdb::Slice LSlice;
typedef rocksdb::WriteOptions LWriteOptions;
typedef rocksdb::ReadOptions LReadOptions;
typedef rocksdb::WriteBatch LWriteBatch;

//// typedef of vertex label and edge label.
//typedef std::string VLabel;
//typedef std::string Elabel;

// Length type
typedef __uint64_t __machine_t;
typedef __uint16_t __block_id_t;
typedef __uint64_t __vertex_id_t;
typedef __uint32_t __label_id_t;
typedef __uint64_t __time_t;
typedef __uint16_t __op_flag_t;

enum OpFlag: __op_flag_t {
    Insert = 1,
    Delete = 0,
};


}
