//
// Created by tramboo on 2020/3/6.
//

#include "ldb.hh"
#include <cmath>

namespace lightgraph
{

LDB::~LDB() {
    delete _db;
}

// Setup
LStatus LDB::Open(const LOptions& loptions, const std::string& dbpath) {
    return rocksdb::DB::Open(loptions, dbpath, &_db);
}

void LDB::SetSchema(Schema &schema) {
    _edge_label_id_map = schema.InnerCoding();
}

// Vertex operations
LStatus LDB::VertexPut(__vertex_id_t vertex_id, const Properties &prop, const LWriteOptions &options) {
    __machine_t block_id = BlockId::Vertex;
    // Fill the padding area
    block_id = block_id << (sizeof(__machine_t) - sizeof(__block_id_t));
    // Assemble keys
    char* key = new char[VertexKeyLen];
    memcpy(key, &block_id, sizeof(__machine_t));
    memcpy(key + sizeof(__machine_t), &vertex_id, sizeof(__vertex_id_t));
    LStatus s = _db->Put(options, LSlice(key, VertexKeyLen), prop.AsString());
    delete[] key;
    return s;
}

} // end namespace lightgraph


