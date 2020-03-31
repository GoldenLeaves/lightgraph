//
// Created by tramboo on 2020/3/6.
//

#include "ldb.hh"
#include <cstdlib>
#include <iostream>

namespace lightgraph
{

LDB::LDB(const Schema &schema): _db(nullptr) {
    schema.InnerCoding(_inner_map);
}

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
    schema.InnerCoding(_inner_map);
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

LStatus LDB::VertexGet(__vertex_id_t vertex_id, std::string* value, const LReadOptions &options)
{
    auto key = GetInnerKeyOfVertex(vertex_id);
    auto s = _db->Get(options, key, value);
    free((void *) key.data());
    return s;
}

std::unique_ptr<LDB::VertexIterator> LDB::VertexScan(const LReadOptions &options) {
    auto prefix = malloc(sizeof(__block_id_t));
    auto block_id = BlockId::Vertex;
    memcpy(static_cast<char*>(prefix), &block_id, sizeof(__block_id_t));
    LSlice prefix_slice(static_cast<char*>(prefix), sizeof(__block_id_t));

    auto l_iter = _db->NewIterator(options);
    l_iter->Seek(prefix_slice);
    std::unique_ptr<VertexIterator> v_iter(new VertexScanIterator(l_iter, prefix_slice));
    return v_iter;
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

std::unique_ptr<LDB::DeltaIterator> LDB::DeltaScan(const LReadOptions &options,
        __time_t lower_t, __time_t upper_t) {
    auto prefix = malloc(sizeof(__block_id_t) + sizeof(__time_t));
    auto block_id = BlockId::TimeSortedDelta;
    memcpy(static_cast<char*>(prefix), &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(prefix) + sizeof(__block_id_t),
            &lower_t, sizeof(__time_t));
    LSlice prefix_slice(static_cast<char*>(prefix), sizeof(__block_id_t) + sizeof(__time_t));

    auto l_iter = _db->NewIterator(options);
    l_iter->Seek(prefix_slice);
    std::unique_ptr<DeltaIterator> d_iter(new TimeSortedDeltaScanIterator(
            l_iter, {prefix_slice.data_, sizeof(__block_id_t)}, lower_t, upper_t));
    return d_iter;
}

// Inner conversion
__label_id_t LDB::GetInnerIdOfLabel(const std::string &label)  const
{
    auto iter = _inner_map.label_to_id.find(label);
    if(iter != _inner_map.label_to_id.end()) {
        return iter->second;
    }
    return 0;
}

bool LDB::GetLabelOfInnerId(__label_id_t id, std::string *label)  const
{
    auto iter = _inner_map.id_to_label.find(id);
    if(iter != _inner_map.id_to_label.end()) {
        *label = iter->second;
        return true;
    }
    return false;
}

LSlice LDB::GetInnerKeyOfVertex(__vertex_id_t vertex_id)  const
{
    __machine_t block_id = BlockId::Vertex;
    // Fill the padding area
    block_id = block_id << (sizeof(__machine_t) - sizeof(__block_id_t));
    auto* key = std::malloc(VertexKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__machine_t));
    memcpy(static_cast<char*>(key) + sizeof(__machine_t),
            &vertex_id, sizeof(__vertex_id_t));
    return {static_cast<char*>(key), static_cast<size_t>(VertexKeyLen)};
}

 LSlice LDB::GetInnerKeyOfTimeSortedDelta(const GraphDelta& delta)  const
{
    auto block_id = BlockId::TimeSortedDelta;
    auto label_id = GetInnerIdOfLabel(delta.edge.label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &delta.t, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t),
            &delta.edge.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &delta.edge.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}

void LDB::GetDeltaFromTimeSortedInnerKey(const LSlice& key, GraphDelta* delta)  const
{
    __block_id_t block_id;
    memcpy(reinterpret_cast<char*>(&block_id), key.data_, sizeof(__block_id_t));
    assert(block_id == BlockId::TimeSortedDelta);
    memcpy(reinterpret_cast<char*>(&(delta->t)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__time_t));
    memcpy(reinterpret_cast<char*>(&(delta->edge.src)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t),
            sizeof(__vertex_id_t));
    __label_id_t label_id;
    memcpy(reinterpret_cast<char*>(&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));
    memcpy(reinterpret_cast<char*>(&(delta->edge.dst)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    memcpy(reinterpret_cast<char*>(&(delta->op)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__op_flag_t));
}

LSlice LDB::GetInnerKeyOfOutDeltaByE(const GraphDelta& delta) const
{
    auto block_id = BlockId::OutDeltaByE;
    auto label_id = GetInnerIdOfLabel(delta.edge.label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(block_id),
            &delta.edge.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &delta.edge.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &delta.t, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}

void LDB::GetDeltaFromOutInnerKeyByE(const LSlice &key, GraphDelta *delta) const
{
    __block_id_t block_id;
    memcpy(reinterpret_cast<char*>(&block_id), key.data_, sizeof(__block_id_t));
    assert(block_id == BlockId::OutDeltaByE);
    memcpy(reinterpret_cast<char*>(&(delta->edge.src)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__vertex_id_t));
    __label_id_t label_id;
    memcpy(reinterpret_cast<char*>(&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));
    memcpy(reinterpret_cast<char*>(&delta->edge.dst),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    memcpy(reinterpret_cast<char*>(&delta->t),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__time_t));
    memcpy(reinterpret_cast<char*>(&delta->op),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            sizeof(__op_flag_t));
}

LSlice LDB::GetInnerKeyOfInDeltaByE(const GraphDelta& delta) const
{
    auto block_id = BlockId::InDeltaByE;
    auto label_id = GetInnerIdOfLabel(delta.edge.label);
    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &delta.edge.dst, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &delta.edge.src, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &delta.t, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            &delta.op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), static_cast<size_t>(DeltaKeyLen)};
}

void LDB::GetDeltaFromInInnerKeyByE(const LSlice &key, GraphDelta *delta) const
{
    __block_id_t block_id;
    memcpy(reinterpret_cast<char*>(&block_id), key.data_, sizeof(__block_id_t));
    assert(block_id == BlockId::InDeltaByE);
    memcpy(reinterpret_cast<char*>(&(delta->edge.dst)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__vertex_id_t));
    __label_id_t label_id;
    memcpy(reinterpret_cast<char*>(&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));
    memcpy(reinterpret_cast<char*>(&delta->edge.src),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    memcpy(reinterpret_cast<char*>(&delta->t),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__time_t));
    memcpy(reinterpret_cast<char*>(&delta->op),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            sizeof(__op_flag_t));
}

} // end namespace lightgraph


