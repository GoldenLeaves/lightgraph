//
// Created by tramboo on 2020/2/24.
//

#include "schema.hh"
#include "properties.hh"
#include "typedef.hh"
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
    Null = 0,
    Insert = 1,
    Delete = 2,
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
    LDB(): _db(nullptr) {}
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

    // Vertex Operations
    LStatus VertexPut(__vertex_id_t vertex_id, const Properties& prop,
            const LWriteOptions& options = LWriteOptions());
    LStatus VertexWriteBatch(const std::vector<std::pair<__vertex_id_t, Properties> >& vertices,
            const LWriteOptions& options = LWriteOptions());
    LStatus VertexGet(__vertex_id_t vertex_id, std::string* value,
            const LReadOptions& options = LReadOptions());
    std::unique_ptr<VertexIterator> VertexScan(const LReadOptions& options = LReadOptions());

    // Delta Operations
    LStatus DeltaPut(const GraphDelta& delta, const Properties& prop,
            const LWriteOptions& options = LWriteOptions());
    std::unique_ptr<DeltaIterator> DeltaScan(const LReadOptions& options = LReadOptions(),
            __time_t lower_t = 0,
            __time_t upper_t = std::numeric_limits<__time_t >::max());

private:
    // Inner iterator class
    class VertexScanIterator;
    class TimeSortedDeltaScanIterator;

    inline __label_id_t GetInnerIdOfLabel(const std::string& label) const;
    inline bool GetLabelOfInnerId(__label_id_t id, std::string* label) const;

    inline LSlice GetInnerKeyOfVertex(__vertex_id_t vertex_id) const;

    inline LSlice GetInnerKeyOfTimeSortedDelta(const GraphDelta& delta) const;
    void GetDeltaFromTimeSortedInnerKey(const LSlice& key, GraphDelta* delta) const;

    inline LSlice GetInnerKeyOfOutDeltaByE(const GraphDelta& delta) const;
    void GetDeltaFromOutInnerKeyByE(const LSlice& key, GraphDelta* delta) const;

    inline LSlice GetInnerKeyOfInDeltaByE(const GraphDelta& delta) const;
    void GetDeltaFromInInnerKeyByE(const LSlice& key, GraphDelta* delta) const;
private:
    rocksdb::DB* _db;
    InnerMap _inner_map;

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
    };
    const int VertexKeyLen = sizeof(__machine_t) + sizeof(__vertex_id_t);
    const int DeltaKeyLen = sizeof(__block_id_t) + sizeof(__vertex_id_t) * 2
            + sizeof(__label_id_t) + sizeof(__time_t) + sizeof(__op_flag_t);
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

    virtual bool GetNext(__vertex_id_t& target) = 0;

};

class LDB::VertexScanIterator: public LDB::VertexIterator {
public:
    VertexScanIterator(): VertexIterator() {}
    explicit VertexScanIterator(LIterator* iter, const LSlice& prefix)
            : VertexIterator(iter, prefix){}
    bool GetNext(__vertex_id_t& target) override;
};

class LDB::DeltaIterator {
protected:
    std::unique_ptr<LIterator> _iter;
    LSlice _prefix;
    __time_t _lower_t;
    __time_t _upper_t;
public:
    DeltaIterator()
        : _iter(nullptr), _prefix()
        , _lower_t(0), _upper_t(std::numeric_limits<__time_t >::max()) {}
    DeltaIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : _iter(iter), _prefix(prefix), _lower_t(lower_t), _upper_t(upper_t) {}
    DeltaIterator(const DeltaIterator&) = delete;
    void operator=(const DeltaIterator&) = delete;
    virtual ~DeltaIterator();

    virtual bool GetNext(const LDB& db, GraphDelta& target) = 0;
};

class LDB::TimeSortedDeltaScanIterator: public LDB::DeltaIterator {
public:
    TimeSortedDeltaScanIterator() : DeltaIterator() {}
    TimeSortedDeltaScanIterator(LIterator* iter, const LSlice& prefix,
            const __time_t lower_t = 0,
            const __time_t upper_t = std::numeric_limits<__time_t >::max())
        : DeltaIterator(iter, prefix, lower_t, upper_t) {}

    bool GetNext(const LDB& db, GraphDelta& target) override;
};

} // end namespace lightgraph