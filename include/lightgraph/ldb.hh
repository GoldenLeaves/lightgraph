//
// Created by tramboo on 2020/2/24.
//

#include "schema.hh"
#include "properties.hh"
#include "typedef.hh"
#include "lightgraph/endian_conversion.hh"
#include <utility>
#include <memory>
#include <limits>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

namespace lightgraph
{

struct GraphEdge {
    __vertex_id_t src;
    std::string label;
    __vertex_id_t dst;

    GraphEdge()
        : src(0), label(""), dst(0) {}
    GraphEdge(__vertex_id_t src, std::string label, __vertex_id_t dst)
        : src(src), label(std::move(label)), dst(dst) {}
    GraphEdge(__vertex_id_t src, const char* label, __vertex_id_t dst)
        : src(src), label(label), dst(dst) {}
    GraphEdge(const GraphEdge&) = default;
    GraphEdge& operator=(const GraphEdge&) = default;
};

enum OpFlag: __op_flag_t {
    Insert = 1,
    Delete = 2,
    Null = std::numeric_limits<__op_flag_t>::max(),
};

struct GraphDelta {
    GraphEdge edge;
    __time_t t;
    OpFlag op;

    GraphDelta()
        : edge(), t(0), op(OpFlag::Null) {}
    GraphDelta(__vertex_id_t src, std::string edge_label, __vertex_id_t dst, __time_t time, OpFlag op_flag)
        : edge(src, std::move(edge_label), dst), t(time), op(op_flag) {}
    GraphDelta(__vertex_id_t src, const char* edge_label, __vertex_id_t dst, __time_t time, OpFlag op_flag)
        : edge(src, edge_label, dst), t(time), op(op_flag) {}
    GraphDelta(const GraphEdge& edge, __time_t time, OpFlag op_flag)
        : edge(edge), t(time), op(op_flag) {}
    GraphDelta(const GraphDelta&) = default;
    GraphDelta& operator=(const GraphDelta&) = default;
};

class LDB {
public:
    // Construction
    LDB(): _db(nullptr), _using_index(false) {}
    explicit LDB(const Schema& schema);
    // No copying allowed
    LDB(const LDB&) = delete;
    void operator=(const LDB&) = delete;

    // Destruction
    virtual ~LDB();

    // Public Iterator class
    class VertexIterator;
    class DeltaIterator;

    // Setup
    LStatus Open(const LOptions& loptions, const std::string& dbpath);
    void SetSchema(const Schema& schema);
    void UsingIndex();
    void ClosingIndex();

    // Vertex Operations
    LStatus VertexPut(__vertex_id_t vertex_id, const Properties& prop,
            const LWriteOptions& options = LWriteOptions());

    LStatus VertexWriteBatch(const std::vector<std::pair<__vertex_id_t, Properties> >& vertices,
            const LWriteOptions& options = LWriteOptions());

    LStatus VertexGet(__vertex_id_t vertex_id, std::string* value,
            const LReadOptions& options = LReadOptions());

    std::unique_ptr<VertexIterator>
    VertexScan(const LReadOptions& options = LReadOptions());

    // Delta Operations
    LStatus DeltaPut(const GraphDelta& delta, const Properties& prop,
            const LWriteOptions& options = LWriteOptions());

    std::unique_ptr<DeltaIterator>
    DeltaScan(__time_t lower_t = 0, __time_t upper_t = std::numeric_limits<__time_t >::max(),
            const LReadOptions& options = LReadOptions());

    std::unique_ptr<DeltaIterator>
    DeltaScanOfOutV(__vertex_id_t src, const std::string& edge_label,
            __time_t lower_t = 0, __time_t upper_t = std::numeric_limits<__time_t >::max(),
            const LReadOptions& options = LReadOptions());

    std::unique_ptr<DeltaIterator>
    DeltaScanOfEdge(const GraphEdge& edge, __time_t lower_t = 0,
            __time_t upper_t = std::numeric_limits<__time_t >::max(),
            const LReadOptions& options = LReadOptions());

    // Graph Operations
    std::unique_ptr<VertexIterator>
    GetOutV(__vertex_id_t src, const std::string& edge_label, __time_t current_time,
            __time_t time_weight = 0, __time_t latest_arrival_time = std::numeric_limits<__time_t >::max(),
            const LReadOptions& options = LReadOptions());

    std::unique_ptr<VertexIterator>
    GetOutVDuring(__vertex_id_t src, const std::string& edge_label,
            __time_t lower_t = 0, __time_t upper_t = std::numeric_limits<__time_t >::max(),
            const LReadOptions& options = LReadOptions());

    std::unique_ptr<VertexIterator>
    GetOutVAt(__vertex_id_t src, const std::string& edge_label,
            __time_t time, const LReadOptions& options = LReadOptions());

//    __time_t GetActiveTimeOfEdge(const GraphEdge& edge,
//            __time_t lower_t = 0, __time_t upper_t = std::numeric_limits<__time_t >::max(),
//            const LReadOptions& options = LReadOptions());
    bool EdgeExists(const GraphEdge& edge, __time_t time,
            const LReadOptions& options = LReadOptions());

private:
    // Inner(private) iterator class
    class VertexScanIterator;
    class OutVIterator;
    class IntervalOutVIterator;
    class OptimizedIntervalOutVIterator;
    class SnapshotOutVIterator;
    class OptimizedSnapshotOutVIterator;
    class TimeSortedDeltaScanIterator;
    class OutEdgeDeltaScanIterator;
    class OutVDeltaScanIterator;

    inline __label_id_t GetInnerIdOfLabel(const std::string& label) const;
    inline bool GetLabelOfInnerId(__label_id_t id, std::string* label) const;

    inline LSlice GetInnerKeyOfVertex(__vertex_id_t vertex_id) const;

    inline LSlice GetInnerKeyOfTimeSortedDelta(const GraphDelta& delta) const;
    void GetDeltaFromTimeSortedInnerKey(const LSlice& key, GraphDelta* delta) const;

    inline LSlice GetInnerKeyOfOutDeltaByE(const GraphDelta& delta) const;
    void GetDeltaFromOutInnerKeyByE(const LSlice& key, GraphDelta* delta) const;

    inline LSlice GetInnerKeyOfInDeltaByE(const GraphDelta& delta) const;
    void GetDeltaFromInInnerKeyByE(const LSlice& key, GraphDelta* delta) const;

    inline LSlice GetInnerKeyOfOutDeltaIndex(const GraphEdge& edge) const;
    inline LSlice GetInnerKeyOfInDeltaIndex(const GraphEdge& edge) const;

private:
    rocksdb::DB* _db;
    InnerMap _inner_map;
    bool _using_index;

    enum BlockId: __block_id_t {
        /* Vertex block
         ---------------key length: 16 Bytes bit------------
         |   0 ~ 1   |     2 ~ 7     |     8 ~ 15     |
         ----------------------------------------------
         |  BlockId  |  NullPadding  |    VertexId    |
         ----------------------------------------------
         */
        Vertex = 0,
        /*
         Delta series sorted by time
         ----------------------------------key length: 32 Bytes---------------------------------
         |   0 ~ 1   |    2 ~ 9   |    10 ~ 17    |    18 ~ 21    |    22 ~ 29    |   30 ~ 31  |
         ---------------------------------------------------------------------------------------
         |  BlockId  |    Time    |  SrcVertexId  |  EdgeLabelId  |  DstVertexId  |   OpFlag   |
         ---------------------------------------------------------------------------------------
         */
        TimeSortedDelta = 1,
        /*
         Out Edge Delta clustered by DstVertex
         ----------------------------------key length: 32 Bytes---------------------------------
         |   0 ~ 1   |     2 ~ 9     |    10 ~ 13    |    14 ~ 21    |   22 ~ 29  |   30 ~ 31  |
         ---------------------------------------------------------------------------------------
         |  BlockId  |  SrcVertexId  |  EdgeLabelId  |  DstVertexId  |    Time    |   OpFlag   |
         ---------------------------------------------------------------------------------------
         */
        OutDeltaByE = 2,
        /*
         In Edge delta clustered by SrcVertex
         ----------------------------------key length: 32 Bytes---------------------------------
         |   0 ~ 1   |     2 ~ 9     |    10 ~ 13    |    14 ~ 21    |   22 ~ 29  |   30 ~ 31  |
         ---------------------------------------------------------------------------------------
         |  BlockId  |  DstVertexId  |  EdgeLabelId  |  SrcVertexId  |    Time    |   OpFlag   |
         ---------------------------------------------------------------------------------------
         */
        InDeltaByE = 3,
        /* TODO */
        OutDeltaByT = 4,
        /* TODO */
        InDeltaByT = 5,
        /*
         Index with Out Vertex
         -----------------------------key length: 24 Bytes----------------------------
         |   0 ~ 1   |     2 ~ 9     |    10 ~ 13    |    14 ~ 21    |    22 ~ 23    |
         -----------------------------------------------------------------------------
         |  BlockId  |  SrcVertexId  |  EdgeLabelId  |  DstVertexId  |  NullPadding  |
         -----------------------------------------------------------------------------
         */
        OutVIndex = 6,
        /*
         Index with In Vertex
         -----------------------------key length: 24 Bytes----------------------------
         |   0 ~ 1   |     2 ~ 9     |    10 ~ 13    |    14 ~ 21    |    22 ~ 23    |
         -----------------------------------------------------------------------------
         |  BlockId  |  DstVertexId  |  EdgeLabelId  |  SrcVertexId  |  NullPadding  |
         -----------------------------------------------------------------------------
         */
        InVIndex = 7,
    };
    const size_t VertexKeyLen = sizeof(__machine_t) + sizeof(__vertex_id_t);
    const size_t DeltaKeyLen = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t) + + sizeof(__vertex_id_t)
            + sizeof(__time_t) + sizeof(__op_flag_t);
    const size_t DeltaIndexKeyLen = sizeof(__block_id_t) + sizeof(__index_padding_t)
            + sizeof(__vertex_id_t) + sizeof(__label_id_t) + + sizeof(__vertex_id_t);
};

class LDB::VertexIterator {
protected:
    std::unique_ptr<LIterator> _iter;
    LSlice _prefix;
public:
    VertexIterator()
        : _iter(nullptr), _prefix() {}
    explicit VertexIterator(LIterator* iter, const LSlice& prefix)
        : _iter(iter), _prefix(prefix) {}
    VertexIterator(const VertexIterator&) = delete;
    void operator=(const VertexIterator&) = delete;
    virtual ~VertexIterator();

    // Need to Override
    // Usage:
    //     auto vertex_iter = db.ScanFunction(...);
    //     __vertex_id_t vertex;
    //     while(vertex_iter->GetNext(vertex)) {
    //         do something;
    //     }
    virtual bool GetNext(__vertex_id_t& target) = 0;
    virtual bool GetNext(__vertex_id_t& target, __time_t& arrival_time);
};

class LDB::VertexScanIterator: public LDB::VertexIterator {
public:
    VertexScanIterator(): VertexIterator() {}
    explicit VertexScanIterator(LIterator* iter, const LSlice& prefix)
            : VertexIterator(iter, prefix){}
    bool GetNext(__vertex_id_t& target) override;
};

class LDB::OutVIterator: public LDB::VertexIterator {
    std::unique_ptr<LIterator> _index_iter;
    LSlice _index_prefix;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
    __time_t _cur_time;
    __time_t _time_weight;
    __time_t _latest_arr_time;
public:
    OutVIterator()
        : VertexIterator(), _index_iter(nullptr), _index_prefix()
        , _cur_time(0), _time_weight(0), _latest_arr_time(std::numeric_limits<__time_t >::max()) {}
    OutVIterator(LIterator* iter, const LSlice& prefix,
            LIterator* index_iter, const LSlice& index_prefix,
            __time_t current_time, __time_t time_weight,
            __time_t latest_arrival_time)
        : VertexIterator(iter, prefix), _index_iter(index_iter), _index_prefix(index_prefix)
        , _cur_time(current_time), _time_weight(time_weight), _latest_arr_time(latest_arrival_time) {}
    ~OutVIterator() override;

    bool GetNext(__vertex_id_t& target) override;
    bool GetNext(__vertex_id_t& target, __time_t& arrival_time) override;
private:
    bool ProcessOneStep(__vertex_id_t& target, __time_t& arrival_time);
};

class LDB::IntervalOutVIterator: public LDB::VertexIterator {
    __time_t _lower_t;
    __time_t _upper_t;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
public:
    IntervalOutVIterator()
        : VertexIterator(), _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    IntervalOutVIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : VertexIterator(iter, prefix), _lower_t(lower_t), _upper_t(upper_t) {}

    bool GetNext(__vertex_id_t& target) override;
};

class LDB::OptimizedIntervalOutVIterator: public LDB::VertexIterator {
    std::unique_ptr<LIterator> _index_iter;
    LSlice _index_prefix;
    __time_t _lower_t;
    __time_t _upper_t;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t);
public:
    OptimizedIntervalOutVIterator()
        : VertexIterator(), _index_iter(nullptr), _index_prefix()
        , _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    OptimizedIntervalOutVIterator(LIterator* iter, const LSlice& prefix,
            LIterator* index_iter, const LSlice& index_prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : VertexIterator(iter, prefix), _index_iter(index_iter), _index_prefix(index_prefix)
        , _lower_t(lower_t), _upper_t(upper_t) {}
    ~OptimizedIntervalOutVIterator() override;

    bool GetNext(__vertex_id_t& target) override;
private:
    bool ProcessOneStep(__vertex_id_t& target);
};

class LDB::SnapshotOutVIterator: public LDB::VertexIterator {
    __time_t _t;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t);
public:
    SnapshotOutVIterator() = delete;
    SnapshotOutVIterator(LIterator* iter, const LSlice& prefix, __time_t t)
        : VertexIterator(iter, prefix), _t(t) {}

    bool GetNext(__vertex_id_t& target) override;
private:
    bool ProcessOneStep(__vertex_id_t& target);
};

class LDB::OptimizedSnapshotOutVIterator: public LDB::VertexIterator {
    std::unique_ptr<LIterator> _index_iter;
    LSlice _index_prefix;
    __time_t _t;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t)
            + sizeof(__label_id_t);
public:
    OptimizedSnapshotOutVIterator() = delete;
    OptimizedSnapshotOutVIterator(LIterator* iter, const LSlice& prefix,
            LIterator* index_iter, const LSlice& index_prefix, __time_t t)
        : VertexIterator(iter, prefix)
        , _index_iter(index_iter), _index_prefix(index_prefix), _t(t) {}
    ~OptimizedSnapshotOutVIterator() override;

    bool GetNext(__vertex_id_t& target) override;
private:
    bool ProcessOneStep(__vertex_id_t& target);
};

class LDB::DeltaIterator {
protected:
    std::unique_ptr<LIterator> _iter;
    LSlice _prefix;
public:
    DeltaIterator()
        : _iter(nullptr), _prefix() {}
    DeltaIterator(LIterator* iter, const LSlice& prefix)
        : _iter(iter), _prefix(prefix) {}
    DeltaIterator(const DeltaIterator&) = delete;
    void operator=(const DeltaIterator&) = delete;
    virtual ~DeltaIterator();

    // Need to Override
    // Usage:
    //     auto edge_iter = db.ScanFunction(...);
    //     GraphDelta delta;
    //     while(edge_iter->GetNext(db, &delta)) {
    //         do something;
    //     }
    virtual bool GetNext(const LDB& db, GraphDelta* target) = 0;
};

class LDB::TimeSortedDeltaScanIterator: public LDB::DeltaIterator {
    __time_t _lower_t;
    __time_t _upper_t;
public:
    TimeSortedDeltaScanIterator()
        : DeltaIterator(), _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    TimeSortedDeltaScanIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : DeltaIterator(iter, prefix), _lower_t(lower_t), _upper_t(upper_t) {}

    bool GetNext(const LDB& db, GraphDelta* target) override;
};

class LDB::OutEdgeDeltaScanIterator: public LDB::DeltaIterator {
    __time_t _lower_t;
    __time_t _upper_t;
public:
    OutEdgeDeltaScanIterator()
        : DeltaIterator(), _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    OutEdgeDeltaScanIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : DeltaIterator(iter, prefix), _lower_t(lower_t), _upper_t(upper_t) {}

    bool GetNext(const LDB& db, GraphDelta* target) override;
};

class LDB::OutVDeltaScanIterator: public LDB::DeltaIterator {
    __time_t _lower_t;
    __time_t _upper_t;
    const size_t _prefix_len = sizeof(__block_id_t) + sizeof(__vertex_id_t) + sizeof(__label_id_t);
public:
    OutVDeltaScanIterator()
        : DeltaIterator(), _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    OutVDeltaScanIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : DeltaIterator(iter, prefix), _lower_t(lower_t), _upper_t(upper_t) {}

    bool GetNext(const LDB& db, GraphDelta* target) override;
};

} // end namespace lightgraph