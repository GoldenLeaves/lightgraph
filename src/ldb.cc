//
// Created by tramboo on 2020/3/6.
//

#include "lightgraph/ldb.hh"
#include <cstdlib>
#include <iostream>

namespace lightgraph
{

LDB::LDB(const Schema &schema): _db(nullptr), _using_index(false) {
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
    _inner_map.id_to_label.clear();
    _inner_map.label_to_id.clear();
    schema.InnerCoding(_inner_map);
}

void LDB::UsingIndex() {
    _using_index = true;
}

void LDB::ClosingIndex() {
    _using_index = false;
}

// Vertex operations
LStatus LDB::VertexPut(__vertex_id_t vertex_id, const Properties &prop,
        const LWriteOptions &options)
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

LStatus LDB::VertexGet(__vertex_id_t vertex_id, std::string* value,
        const LReadOptions &options)
{
    auto key = GetInnerKeyOfVertex(vertex_id);
    auto s = _db->Get(options, key, value);
    free((void *) key.data());
    return s;
}

std::unique_ptr<LDB::VertexIterator>
LDB::VertexScan(const LReadOptions &options)
{
    auto virtual_prefix = GetInnerKeyOfVertex(0);
    auto true_prefix_len = sizeof(__block_id_t);
    LSlice prefix_slice(virtual_prefix.data_, true_prefix_len);

    auto l_iter = _db->NewIterator(options);
    l_iter->Seek(prefix_slice);
    std::unique_ptr<VertexIterator> v_iter(
            new VertexScanIterator(l_iter, prefix_slice));
    return v_iter;
}

// Delta Operations
LStatus LDB::DeltaPut(const GraphDelta &delta, const Properties &prop,
        const LWriteOptions &options)
{
    LWriteBatch batch;
    LSlice key;
    std::string value = prop.AsString();

    // Store the value only in TimeSortedDelta block
    key = GetInnerKeyOfTimeSortedDelta(delta);
    batch.Put(key, value);
    free((void *) key.data());

    key = GetInnerKeyOfOutDeltaByE(delta);
    batch.Put(key, "");
    free((void *) key.data());
    if(_using_index) {
        // Add OutDelta Index
        key = GetInnerKeyOfOutDeltaIndex(delta.edge);
        batch.Put(key, "");
        free((void *) key.data());
    }

    key = GetInnerKeyOfInDeltaByE(delta);
    batch.Put(key, value);
    free((void *) key.data());
    if(_using_index) {
        // Add InDelta Index
        key = GetInnerKeyOfInDeltaIndex(delta.edge);
        batch.Put(key, "");
        free((void *) key.data());
    }

    return _db->Write(options, &batch);
}

std::unique_ptr<LDB::DeltaIterator>
LDB::DeltaScan(__time_t lower_t, __time_t upper_t, const LReadOptions &options)
{
    auto virtual_prefix = GetInnerKeyOfTimeSortedDelta({{}, lower_t, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__time_t);
    LSlice prefix_slice(virtual_prefix.data_, true_prefix_len);

    auto l_iter = _db->NewIterator(options);
    l_iter->Seek(prefix_slice);
    std::unique_ptr<DeltaIterator> d_iter(new TimeSortedDeltaScanIterator(
            l_iter, {prefix_slice.data_, sizeof(__block_id_t)}, lower_t, upper_t));
    return d_iter;
}

std::unique_ptr<LDB::DeltaIterator>
LDB::DeltaScanOfOutV(__vertex_id_t src, const std::string &edge_label,
        __time_t lower_t, __time_t upper_t, const LReadOptions &options)
{
    auto virtual_prefix = GetInnerKeyOfOutDeltaByE({{src, edge_label, 0}, 0, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
    LSlice prefix_slice(virtual_prefix.data_, true_prefix_len);

    auto l_iter = _db->NewIterator(options);
    l_iter->Seek(prefix_slice);
    std::unique_ptr<DeltaIterator> d_iter(new OutVDeltaScanIterator(
            l_iter, prefix_slice, lower_t, upper_t));
    return d_iter;
}

std::unique_ptr<LDB::DeltaIterator>
LDB::DeltaScanOfEdge(const GraphEdge& edge, __time_t lower_t, __time_t upper_t,
        const LReadOptions &options)
{
    auto virtual_prefix_slice = GetInnerKeyOfOutDeltaByE({edge, lower_t, OpFlag::Null});
    auto l_iter = _db->NewIterator(options);
    l_iter->Seek({virtual_prefix_slice.data_, DeltaKeyLen - sizeof(__op_flag_t)});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t) + sizeof(__vertex_id_t);
    std::unique_ptr<LDB::DeltaIterator> d_iter(new OutEdgeDeltaScanIterator(
            l_iter, {virtual_prefix_slice.data_, true_prefix_len}, lower_t, upper_t));
    return d_iter;
}

// Graph Operations
std::unique_ptr<LDB::VertexIterator>
LDB::GetOutV(__vertex_id_t src, const std::string &edge_label, __time_t current_time,
        __time_t time_weight, __time_t latest_arrival_time, const LReadOptions &options)
{
    auto virtual_prefix_slice = GetInnerKeyOfOutDeltaByE(
            {src, edge_label, 0, current_time, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
    LSlice true_prefix_slice(virtual_prefix_slice.data_, true_prefix_len);

    auto delta_iter = _db->NewIterator(options);
    std::unique_ptr<VertexIterator> v_iter;

    // Default: using index
    auto index_iter = _db->NewIterator(options);
    auto virtual_index_prefix_slice =
            GetInnerKeyOfOutDeltaIndex({src, edge_label, 0});
    LSlice true_index_prefix_slice(virtual_index_prefix_slice.data_, true_prefix_len);
    index_iter->Seek(true_index_prefix_slice);
    v_iter.reset(new OutVIterator(delta_iter, true_prefix_slice,
            index_iter, true_index_prefix_slice, current_time, time_weight, latest_arrival_time));

    return v_iter;
}

std::unique_ptr<LDB::VertexIterator>
LDB::GetOutVDuring(__vertex_id_t src, const std::string &edge_label,
        __time_t lower_t, __time_t upper_t, const LReadOptions &options)
{
    auto virtual_prefix_slice = GetInnerKeyOfOutDeltaByE(
            {src, edge_label, 0, lower_t, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
    LSlice true_prefix_slice(virtual_prefix_slice.data_, true_prefix_len);

    auto delta_iter = _db->NewIterator(options);
    std::unique_ptr<VertexIterator> v_iter;
    if(_using_index) {
        auto index_iter = _db->NewIterator(options);
        auto virtual_index_prefix_slice =
                GetInnerKeyOfOutDeltaIndex({src, edge_label, 0});
        LSlice true_index_prefix_slice(virtual_index_prefix_slice.data_, true_prefix_len);

        index_iter->Seek(true_index_prefix_slice);
        v_iter.reset(new OptimizedIntervalOutVIterator(delta_iter, true_prefix_slice,
                index_iter, true_index_prefix_slice, lower_t, upper_t));
    } else {
        delta_iter->Seek(true_prefix_slice);
        v_iter.reset(new IntervalOutVIterator(delta_iter, true_prefix_slice, lower_t, upper_t));
    }
    return v_iter;
}

std::unique_ptr<LDB::VertexIterator>
LDB::GetOutVAt(__vertex_id_t src, const std::string &edge_label,
        __time_t time, const LReadOptions &options)
{
    auto virtual_prefix_slice = GetInnerKeyOfOutDeltaByE(
            {src, edge_label, 0, time, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
    LSlice true_prefix_slice(virtual_prefix_slice.data_, true_prefix_len);

    auto delta_iter = _db->NewIterator(options);
    std::unique_ptr<VertexIterator> v_iter;
    if(_using_index) {
        auto index_iter = _db->NewIterator(options);
        auto virtual_index_prefix_slice =
                GetInnerKeyOfOutDeltaIndex({src, edge_label, 0});

        LSlice true_index_prefix_slice(virtual_index_prefix_slice.data_, true_prefix_len);
        index_iter->Seek(true_index_prefix_slice);
        v_iter.reset(new OptimizedSnapshotOutVIterator(delta_iter, true_prefix_slice,
                index_iter, true_index_prefix_slice, time));
    } else {
        delta_iter->Seek(true_prefix_slice);
        v_iter.reset(new SnapshotOutVIterator(delta_iter, true_prefix_slice, time));
    }
    return v_iter;
}

//__time_t LDB::GetActiveTimeOfEdge(const GraphEdge &edge,
//        __time_t lower_t, __time_t upper_t, const LReadOptions &options)
//{
//    return 0;
//}

bool LDB::EdgeExists(const GraphEdge &edge, __time_t time,
        const LReadOptions &options)
{
    auto virtual_prefix_slice = GetInnerKeyOfOutDeltaByE({edge, time, OpFlag::Null});
    auto true_prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t) + sizeof(__vertex_id_t);
    auto l_iter = _db->NewIterator(options);
    l_iter->SeekForPrev({virtual_prefix_slice.data_,
                         true_prefix_len + sizeof(__time_t)});
    if(l_iter->Valid() && l_iter->key().starts_with({virtual_prefix_slice.data_, true_prefix_len})) {
        __op_flag_t op;
        memcpy((void *) (&op),
               l_iter->key().data_ + true_prefix_len + sizeof(__time_t),
               sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);
        if(op == OpFlag::Insert) {
            return true;
        }
    }
    return false;
}

// Inner conversion
__label_id_t LDB::GetInnerIdOfLabel(const std::string &label)  const
{
    auto iter = _inner_map.label_to_id.find(label);
    if(iter != _inner_map.label_to_id.end()) {
        return iter->second;
    }
    return std::numeric_limits<__label_id_t>::max();
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
    auto block_id = EndianConversion::HtoN16(BlockId::Vertex);
    vertex_id = EndianConversion::HtoN64(vertex_id);
    __machine_t id = 0;

    auto* key = std::malloc(VertexKeyLen);
    memcpy(static_cast<char*>(key),
            &id, sizeof(__machine_t));
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__machine_t),
            &vertex_id, sizeof(__vertex_id_t));
    return {static_cast<char*>(key), VertexKeyLen};
}

 LSlice LDB::GetInnerKeyOfTimeSortedDelta(const GraphDelta& delta)  const
{
    auto block_id = EndianConversion::HtoN16(BlockId::TimeSortedDelta);
    auto time = EndianConversion::HtoN64(delta.t);
    auto src_id = EndianConversion::HtoN64(delta.edge.src);
    auto label_id = EndianConversion::HtoN32(GetInnerIdOfLabel(delta.edge.label));
    auto dst_id = EndianConversion::HtoN64(delta.edge.dst);
    auto op = EndianConversion::HtoN16(delta.op);

    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &time, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t),
            &src_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &dst_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), DeltaKeyLen};
}

void LDB::GetDeltaFromTimeSortedInnerKey(const LSlice& key, GraphDelta* delta)  const
{
    __block_id_t block_id;
    memcpy((void *) (&block_id), key.data_, sizeof(__block_id_t));
    assert(EndianConversion::NtoH16(block_id) == BlockId::TimeSortedDelta);

    memcpy((void *) (&(delta->t)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__time_t));
    delta->t = EndianConversion::NtoH64(delta->t);

    memcpy((void *) (&(delta->edge.src)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t),
            sizeof(__vertex_id_t));
    delta->edge.src = EndianConversion::NtoH64(delta->edge.src);

    __label_id_t label_id;
    memcpy((void *) (&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    label_id = EndianConversion::NtoH32(label_id);
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));

    memcpy((void *) (&(delta->edge.dst)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    delta->edge.dst = EndianConversion::NtoH64(delta->edge.dst);

    memcpy((void *) (&(delta->op)),
            key.data_ + sizeof(__block_id_t) + sizeof(__time_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__op_flag_t));
    delta->op = static_cast<OpFlag>(EndianConversion::NtoH16(delta->op));
}

LSlice LDB::GetInnerKeyOfOutDeltaByE(const GraphDelta& delta) const
{
    auto block_id = EndianConversion::HtoN16(BlockId::OutDeltaByE);
    auto src_id = EndianConversion::HtoN64(delta.edge.src);
    auto label_id = EndianConversion::HtoN32(GetInnerIdOfLabel(delta.edge.label));
    auto dst_id = EndianConversion::HtoN64(delta.edge.dst);
    auto time = EndianConversion::HtoN64(delta.t);
    auto op = EndianConversion::HtoN16(delta.op);

    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &src_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &dst_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &time, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            &op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), DeltaKeyLen};
}

void LDB::GetDeltaFromOutInnerKeyByE(const LSlice &key, GraphDelta *delta) const
{
    __block_id_t block_id;
    memcpy((void *) (&block_id), key.data_, sizeof(__block_id_t));
    assert(EndianConversion::NtoH16(block_id) == BlockId::OutDeltaByE);

    memcpy((void *) (&(delta->edge.src)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__vertex_id_t));
    delta->edge.src = EndianConversion::NtoH64(delta->edge.src);

    __label_id_t label_id;
    memcpy((void *) (&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    label_id = EndianConversion::NtoH32(label_id);
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));

    memcpy((void *) (&delta->edge.dst),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    delta->edge.dst = EndianConversion::NtoH64(delta->edge.dst);

    memcpy((void *) (&delta->t),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__time_t));
    delta->t = EndianConversion::NtoH64(delta->t);

    memcpy((void *) (&delta->op),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            sizeof(__op_flag_t));
    delta->op = static_cast<OpFlag>(EndianConversion::NtoH16(delta->op));
}

LSlice LDB::GetInnerKeyOfInDeltaByE(const GraphDelta& delta) const
{
    auto block_id = EndianConversion::HtoN16(BlockId::OutDeltaByE);
    auto src_id = EndianConversion::HtoN64(delta.edge.src);
    auto label_id = EndianConversion::HtoN32(GetInnerIdOfLabel(delta.edge.label));
    auto dst_id = EndianConversion::HtoN64(delta.edge.dst);
    auto time = EndianConversion::HtoN64(delta.t);
    auto op = EndianConversion::HtoN16(delta.op);

    auto* key = std::malloc(DeltaKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &dst_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &src_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &time, sizeof(__time_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            &op, sizeof(__op_flag_t));
    return {static_cast<char*>(key), DeltaKeyLen};
}

void LDB::GetDeltaFromInInnerKeyByE(const LSlice &key, GraphDelta *delta) const
{
    __block_id_t block_id;
    memcpy((void *) (&block_id), key.data_, sizeof(__block_id_t));
    assert(EndianConversion::NtoH16(block_id) == BlockId::InDeltaByE);

    memcpy((void *) (&(delta->edge.dst)),
            key.data_ + sizeof(__block_id_t),
            sizeof(__vertex_id_t));
    delta->edge.dst = EndianConversion::NtoH64(delta->edge.dst);

    __label_id_t label_id;
    memcpy((void *) (&label_id),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            sizeof(__label_id_t));
    label_id = EndianConversion::NtoH32(label_id);
    assert(GetLabelOfInnerId(label_id, &(delta->edge.label)));

    memcpy((void *) (&delta->edge.src),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            sizeof(__vertex_id_t));
    delta->edge.src = EndianConversion::NtoH64(delta->edge.src);

    memcpy((void *) (&delta->t),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            sizeof(__time_t));
    delta->t = EndianConversion::NtoH64(delta->t);

    memcpy((void *) (&delta->op),
            key.data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t) + sizeof(__time_t),
            sizeof(__op_flag_t));
    delta->op = static_cast<OpFlag>(EndianConversion::NtoH16(delta->op));
}

LSlice LDB::GetInnerKeyOfOutDeltaIndex(const GraphEdge &edge) const
{
    __block_id_t block_id = EndianConversion::HtoN16(BlockId::OutVIndex);
    auto src_id = EndianConversion::HtoN64(edge.src);
    auto label_id = EndianConversion::HtoN32(GetInnerIdOfLabel(edge.label));
    auto dst_id = EndianConversion::HtoN64(edge.dst);
    __index_padding_t padding = 0;

    auto *key = std::malloc(DeltaIndexKeyLen);
    memcpy(static_cast<char*>(key),
            &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
            &src_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
            &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
            &dst_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
            &padding, sizeof(__index_padding_t));
    return {static_cast<char*>(key), DeltaIndexKeyLen};
}

LSlice LDB::GetInnerKeyOfInDeltaIndex(const GraphEdge &edge) const
{
    __block_id_t block_id = EndianConversion::HtoN16(BlockId::InVIndex);
    auto src_id = EndianConversion::HtoN64(edge.src);
    auto label_id = EndianConversion::HtoN32(GetInnerIdOfLabel(edge.label));
    auto dst_id = EndianConversion::HtoN64(edge.dst);
    __index_padding_t padding = 0;


    auto *key = std::malloc(DeltaIndexKeyLen);
    memcpy(static_cast<char*>(key),
           &block_id, sizeof(__block_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t),
           &dst_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t),
           &label_id, sizeof(__label_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t),
           &src_id, sizeof(__vertex_id_t));
    memcpy(static_cast<char*>(key) + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
           &padding, sizeof(__index_padding_t));
    return {static_cast<char*>(key), DeltaIndexKeyLen};
}

} // end namespace lightgraph
