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

bool LDB::VertexIterator::GetNext(__vertex_id_t &target, __time_t &arrival_time) {
    return false;
}

bool LDB::VertexScanIterator::GetNext(__vertex_id_t &target)
{
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    memcpy((void *) (&target),
            _iter->key().data_ + sizeof(__machine_t),
            sizeof(__vertex_id_t));
    target = EndianConversion::NtoH64(target);

    _iter->Next();
    return true;
}

LDB::OutVIterator::~OutVIterator() {
    free((void *) _index_prefix.data_);
}

bool LDB::OutVIterator::GetNext(__vertex_id_t &target) {
    return false;
}

bool LDB::OutVIterator::GetNext(__vertex_id_t &target, __time_t &arrival_time) {
    do {
        if(!_index_iter->Valid() || !_index_iter->key().starts_with(_index_prefix)) {
            return false;
        }
    } while(!ProcessOneStep(target, arrival_time));
    return true;
}

bool LDB::OutVIterator::ProcessOneStep(__vertex_id_t &target, __time_t &arrival_time) {
    memcpy((void *) (_prefix.data_ + _prefix_len),
           _index_iter->key().data_ + _prefix_len,
           sizeof(__vertex_id_t));
    LSlice step_prefix(_prefix.data_, _prefix_len + sizeof(__vertex_id_t));

    _iter->SeekForPrev({_prefix.data_,
                        _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t)});
    if(!_iter->key().starts_with(step_prefix)) {
        _iter->Next();
    }

    bool return_value = false;
    __time_t dep_time, future_arr_time = 0, op_time;
    __op_flag_t op;

    while(_iter->Valid() && _iter->key().starts_with(step_prefix)) {
        memcpy((void *) (&op),
               _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t),
               sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);

        if(op == OpFlag::Insert) {
            assert(!return_value);
            memcpy((void *) (&op_time),
               _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t),
               sizeof(__time_t));
            dep_time = std::max(EndianConversion::NtoH64(op_time), _cur_time);
            future_arr_time = dep_time + _time_weight;
            if(future_arr_time > _latest_arr_time) {
                break;
            }
            return_value = true;
        } else if(op == OpFlag::Delete) {
            if(return_value) {
                memcpy((void *) (&op_time),
                       _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t),
                       sizeof(__time_t));
                op_time = EndianConversion::NtoH64(op_time);
                if(op_time >= future_arr_time) {
                    break;
                } else {
                    return_value = false;
                }
            }
        } else {
            return_value = false;
            break;
        }
        _iter->Next();
    }

    if(return_value) {
        memcpy((void *) (&target),
               _index_iter->key().data_ + _prefix_len,
               sizeof(__vertex_id_t));
        target = EndianConversion::NtoH64(target);
        arrival_time = future_arr_time;
    }
    _index_iter->Next();
    return return_value;
}

bool LDB::IntervalOutVIterator::GetNext(__vertex_id_t &target) {
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    __time_t t;
    __op_flag_t op;
    while(_iter->Valid() && _iter->key().starts_with(_prefix)) {
        memcpy((void *) (&t),
                _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t),
                sizeof(__time_t));
        t = EndianConversion::NtoH64(t);
        memcpy((void *) (&op),
                _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t),
                sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);

        if(op == OpFlag::Insert && (t >= _lower_t && t <= _upper_t)) {
            memcpy((void *) (&target),
                    _iter->key().data_ + _prefix_len,
                    sizeof(__vertex_id_t));
            target = EndianConversion::NtoH64(target);
            memcpy((void *) (_prefix.data_ + _prefix_len),
                   _iter->key().data_ + _prefix_len,
                   sizeof(__vertex_id_t));
            LSlice step_prefix(_prefix.data_, _prefix_len + sizeof(__vertex_id_t));
            while(_iter->Valid() && _iter->key().starts_with(step_prefix)) {
                _iter->Next();
            }
            return true;
        }
        _iter->Next();
    }
    return false;
}

LDB::OptimizedIntervalOutVIterator::~OptimizedIntervalOutVIterator() {
    free((void *) _index_prefix.data_);
}

bool LDB::OptimizedIntervalOutVIterator::GetNext(__vertex_id_t &target) {
    do {
        if(!_index_iter->Valid() || !_index_iter->key().starts_with(_index_prefix)) {
            return false;
        }
    } while(!ProcessOneStep(target));
    return true;
}

bool LDB::OptimizedIntervalOutVIterator::ProcessOneStep(__vertex_id_t &target)
{
    memcpy((void *) (_prefix.data_ + _prefix_len),
           _index_iter->key().data_ + _prefix_len,
           sizeof(__vertex_id_t));

    LSlice step_prefix(_prefix.data_, _prefix_len + sizeof(__vertex_id_t));
    _iter->Seek({_prefix.data_,
                 _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t)});
    __time_t t;
    __op_flag_t op;
    while(_iter->Valid() && _iter->key().starts_with(step_prefix)) {
        memcpy((void *) (&t),
                _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t),
                sizeof(__time_t));
        t = EndianConversion::NtoH64(t);
        memcpy((void *) (&op),
                _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t),
                sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);

        if(t <= _upper_t) {
            if(op == OpFlag::Insert) {
                memcpy((void *) (&target),
                       _index_iter->key().data_ + _prefix_len,
                       sizeof(__vertex_id_t));
                target = EndianConversion::NtoH64(target);

                _index_iter->Next();
                return true;
            }
        } else break;
        _iter->Next();
    }
    _index_iter->Next();
    return false;
}

bool LDB::SnapshotOutVIterator::GetNext(__vertex_id_t &target)
{
    do {
        if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
            return false;
        }
    } while(!ProcessOneStep(target));
    return true;
}

bool LDB::SnapshotOutVIterator::ProcessOneStep(__vertex_id_t &target)
{
    __vertex_id_t dst;
    memcpy((void *) (&dst),
           _iter->key().data_ + _prefix_len,
           sizeof(__vertex_id_t));
    dst = EndianConversion::NtoH64(dst);
    memcpy((void *) (_prefix.data_ + _prefix_len),
           _iter->key().data_ + _prefix_len,
           sizeof(__vertex_id_t));

    LSlice step_prefix(_prefix.data_, _prefix_len + sizeof(__vertex_id_t));
    _iter->SeekForPrev(
            {_prefix.data_, _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t)});

    bool ret_value = false;
    if(_iter->Valid() && _iter->key().starts_with(step_prefix)) {
        __op_flag_t op;
        memcpy((void *) (&op),
               _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t),
               sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);

        if(op == OpFlag::Insert) {
            target = dst;
            ret_value = true;
        }
    }

    auto next_dst_bound = EndianConversion::HtoN64(dst + 1);
    memcpy((void *) (_prefix.data_ + _prefix_len),
           &next_dst_bound,
           sizeof(__vertex_id_t));
    _iter->Seek({_prefix.data_, _prefix_len + sizeof(__vertex_id_t)});
    return ret_value;
}

LDB::OptimizedSnapshotOutVIterator::~OptimizedSnapshotOutVIterator() {
    free((void *) _index_prefix.data_);
}

bool LDB::OptimizedSnapshotOutVIterator::GetNext(__vertex_id_t &target) {
    do {
        if(!_index_iter->Valid() || !_index_iter->key().starts_with(_index_prefix)) {
            return false;
        }
    } while(!ProcessOneStep(target));
    return true;
}

bool LDB::OptimizedSnapshotOutVIterator::ProcessOneStep(__vertex_id_t &target)
{
    memcpy((void *) (_prefix.data_ + _prefix_len),
           _index_iter->key().data_ + _prefix_len,
           sizeof(__vertex_id_t));

    LSlice step_prefix(_prefix.data_, _prefix_len + sizeof(__vertex_id_t));
    _iter->SeekForPrev(
            {_prefix.data_, _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t)});

    if(_iter->Valid() && _iter->key().starts_with(step_prefix)) {
        __op_flag_t op;
        memcpy((void *) (&op),
               _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t) + sizeof(__time_t),
               sizeof(__op_flag_t));
        op = EndianConversion::NtoH16(op);

        if(op == OpFlag::Insert) {
            memcpy((void *) (&target),
                   _index_iter->key().data_ + _prefix_len,
                   sizeof(__vertex_id_t));
            target = EndianConversion::NtoH64(target);

            _index_iter->Next();
            return true;
        }
    }
    _index_iter->Next();
    return false;
}

LDB::DeltaIterator::~DeltaIterator()
{
    free((void *) _prefix.data_);
}

bool LDB::TimeSortedDeltaScanIterator::GetNext(const LDB& db, GraphDelta *target)
{
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    __time_t time;
    memcpy((void *) (&time),
            _iter->key().data_ + sizeof(__block_id_t),
            sizeof(__time_t));
    time = EndianConversion::NtoH64(time);
    if(time < _lower_t || time > _upper_t) {
        return false;
    }
    db.GetDeltaFromTimeSortedInnerKey(_iter->key(), target);
    _iter->Next();
    return true;
}

bool LDB::OutEdgeDeltaScanIterator::GetNext(const LDB &db, GraphDelta *target)
{
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }

    __time_t time;
    memcpy((void *) (&time),
           _iter->key().data_ + sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t) + sizeof(__vertex_id_t),
           sizeof(__time_t));
    time = EndianConversion::NtoH64(time);
    if(time < _lower_t || time > _upper_t) {
        return false;
    }
    db.GetDeltaFromOutInnerKeyByE(_iter->key(), target);
    _iter->Next();
    return true;
}

bool LDB::OutVDeltaScanIterator::GetNext(const LDB &db, GraphDelta *target)
{
    if(!_iter->Valid() || !_iter->key().starts_with(_prefix)) {
        return false;
    }
    __time_t t;
    while(_iter->Valid() && _iter->key().starts_with(_prefix)) {
        memcpy((void *) (&t),
                _iter->key().data_ + _prefix_len + sizeof(__vertex_id_t),
                sizeof(__time_t));
        t = EndianConversion::NtoH64(t);
        if(t >= _lower_t && t <= _upper_t) {
            db.GetDeltaFromOutInnerKeyByE(_iter->key(), target);
            _iter->Next();
            return true;
        }
        _iter->Next();
    }
    return false;
}

} // end namespace lightgraph