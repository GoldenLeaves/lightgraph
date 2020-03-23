//
// Created by tramboo on 2020/3/6.
//

#include "ldb.hh"
#include <cstdlib>
#include <cmath>

namespace lightgraph
{

LDB::~LDB()
{
    delete _db;
}

// Setup
LStatus LDB::Open(const LOptions& loptions, const std::string& dbpath)
{
    return rocksdb::DB::Open(loptions, dbpath, &_db);
}

void LDB::SetSchema(const Schema &schema)
{
    _edge_label_id_map = schema.InnerCoding();
}

// Vertex operations
LStatus LDB::VertexPut(__vertex_id_t vertex_id, const Properties &prop, const LWriteOptions &options)
{
    auto key = GetInnerKeyOfVertex(vertex_id);
    LStatus s = _db->Put(options, key, prop.AsString());
    free((void *) key.data());
    return s;
}

LStatus LDB::VertexWriteBatch(const std::vector<std::pair<__vertex_id_t, Properties> > &vertices,
        const LWriteOptions &options)
{
    LWriteBatch batch;
    LSlice key;
    for(auto& v: vertices) {
        key = GetInnerKeyOfVertex(v.first);
        batch.Put(key, v.second.AsString());
        free((void *) key.data());
    }
    return _db->Write(options, &batch);
}

LStatus LDB::VertexGet(__vertex_id_t vertex_id, Properties *props, const LReadOptions &options)
{
    auto key = GetInnerKeyOfVertex(vertex_id);
    std::string value;
    auto s = _db->Get(options, key, &value);
    free((void *) key.data());
    if(s.ok()) {
        props->LoadFrom(value);
    }
    return s;
}

// Delta Operations
LStatus LDB::DeltaPut(const GraphDelta &delta, const Properties &prop, const LWriteOptions &options)
{
    LWriteBatch batch;
    LSlice key;
    std::string value = prop.AsString();

    key = GetInnerKeyOfTimeSortedDelta(delta);
    batch.Put(key, value);
    free((void *) key.data());

    key = GetInnerKeyOfOutDeltaByE(delta);
    batch.Put(key, value);
    free((void *) key.data());

    key = GetInnerKeyOfInDeltaByE(delta);
    batch.Put(key, value);
    free((void *) key.data());

    return _db->Write(options, &batch);
}

// Inner conversion
__label_id_t LDB::GetInnerLabelId(const std::string &label) {
    return _edge_label_id_map[label];
}

LSlice LDB::GetInnerKeyOfVertex(__vertex_id_t vertex_id)
{
    __machine_t block_id = BlockId::Vertex;
    // Fill the padding area
    block_id = block_id << (sizeof(__machine_t) - sizeof(__block_id_t));
    auto* key = std::malloc(VertexKeyLen);
    memcpy(static_cast<char*>(key), &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t), &vertex_id, sizeof(__vertex_id_t));
    return {static_cast<char*>(key), static_cast<size_t>(VertexKeyLen)};
}
 LSlice LDB::GetInnerKeyOfTimeSortedDelta(const GraphDelta& delta)
{
    auto block_id = BlockId::TimeSortedDelta;
    auto label_id = GetInnerLabelId(delta.edge_label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key), &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t), &delta.t, sizeof(delta.t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t),
            &delta.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &delta.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}

LSlice LDB::GetInnerKeyOfOutDeltaByE(const GraphDelta& delta)
{
    auto block_id = BlockId::OutDeltaByE;
    auto label_id = GetInnerLabelId(delta.edge_label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key), &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(block_id), &delta.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t), &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
           &delta.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
           &delta.t, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
           &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}

LSlice LDB::GetInnerKeyOfInDeltaByE(const GraphDelta& delta)
{
    auto block_id = BlockId::InDeltaByE;
    auto label_id = GetInnerLabelId(delta.edge_label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key), &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t), &delta.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t), &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
           &delta.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
           &delta.t, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
           &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}


} // end namespace lightgraph


