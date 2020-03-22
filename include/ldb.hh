//
// Created by tramboo on 2020/2/24.
//

#include "typedef.hh"
#include "schema.hh"
#include "properties.hh"

namespace lightgraph
{

class LDB {
    rocksdb::DB* _db;
    std::unordered_map<std::string, int> _edge_label_id_map;

    enum BlockId: __block_id_t {
        /* Vertex block
         ---------------key length: 128 bit------------
         |   0 ~ 15  |    16 ~ 63    |    64 ~ 127    |
         ----------------------------------------------
         |  BlockId  |  NullPadding  |    VertexId    |
         ----------------------------------------------
         */
        Vertex = 0,

        /*
         Delta series sorted by time
         ----------------------------------key length: 256 bit----------------------------------
         |   0 ~ 15  |   16 ~ 79  |   80 ~ 143    |   144 ~ 175   |   176 ~ 239   |  240 ~ 255 |
         ---------------------------------------------------------------------------------------
         |  BlockId  |    Time    |  SrcVertexId  |  EdgeLabelId  |  DstVertexId  |   OpFlag   |
         ---------------------------------------------------------------------------------------
         */
        TimeSortedDelta = 1,

        /*
         Out Edge Delta clustered by DstVertex
         ----------------------------------key length: 256 bit----------------------------------
         |   0 ~ 15  |    16 ~ 79    |    80 ~ 111   |   112 ~ 175   |  176 ~ 239 |  240 ~ 255 |
         ---------------------------------------------------------------------------------------
         |  BlockId  |  SrcVertexId  |  EdgeLabelId  |  DstVertexId  |    Time    |   OpFlag   |
         ---------------------------------------------------------------------------------------
         */
        OutDeltaByE = 2,

        /*
         In Edge delta clustered by SrcVertex
         ----------------------------------key length: 256 bit----------------------------------
         |   0 ~ 15  |    16 ~ 79    |    80 ~ 111   |   112 ~ 175   |  176 ~ 239 |  240 ~ 255 |
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
    const int DeltaKeyLen = sizeof(__block_id_t) + sizeof(__vertex_id_t)*2
            + sizeof(__label_id_t) + sizeof(__time_t) + sizeof(__op_flag_t);

public:
    // Construction
    LDB(): _db(nullptr) {}
    explicit LDB(Schema& schema)
        : _db(nullptr), _edge_label_id_map(schema.InnerCoding()) {}
    // No copying allowed
    LDB(const LDB&) = delete;
    void operator=(const LDB&) = delete;

    // Destruction
    virtual ~LDB();

    // Setup
    LStatus Open(const LOptions& loptions, const std::string& dbpath);
    void SetSchema(Schema& schema);

    // Vertex Operations
    LStatus VertexPut(__vertex_id_t vertex_id, const Properties& prop,
            const LWriteOptions& options = rocksdb::WriteOptions());
    LStatus VertexGet(__vertex_id_t vertex_id, Properties* props,
            const LWriteOptions& options = rocksdb::WriteOptions());

    // Delta Operations

};

} // end namespace lightgraph