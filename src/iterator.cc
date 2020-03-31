//
// Created by tramboo on 2020/3/31.
//

#include "ldb.hh"

namespace lightgraph
{

LDB::VertexIterator::~VertexIterator()
{
    free((void *) _prefix.data_);
}

bool LDB::VertexScanIterator::GetNext(__vertex_id_t &target)
{
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    memcpy(reinterpret_cast<char*>(&target),
            _iter->key().data_ + sizeof(__machine_t), sizeof(__vertex_id_t));
    _iter->Next();
    return true;
}

LDB::DeltaIterator::~DeltaIterator() {
    free((void *) _prefix.data_);
}

bool LDB::TimeSortedDeltaScanIterator::GetNext(const LDB& db, GraphDelta &target) {
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    __time_t time;
    memcpy(reinterpret_cast<char*>(&time),
            _iter->key().data_ + sizeof(__block_id_t), sizeof(__time_t));
    if(time < _lower_t || time > _upper_t) {
        return false;
    }
    db.GetDeltaFromTimeSortedInnerKey(_iter->key(), &target);
    _iter->Next();
    return true;
}

} // end namespace lightgraph