//
// Created by tramboo on 2020/2/24.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include "schema.hh"

namespace lightgraph
{

typedef rocksdb::Status LStatus;
typedef rocksdb::Options LOptions;

class LDB {
    rocksdb::DB* _db;
    std::unordered_map<std::string, int> _label_id_map;
public:
    // Construction
    LDB(): _db(nullptr) {}
    explicit LDB(Schema& schema)
        : _db(nullptr), _label_id_map(schema.InnerCoding()) {}
    // No copying allowed
    LDB(const LDB&) = delete;
    void operator=(const LDB&) = delete;

    // Destruction
    virtual ~LDB();

    // Setup
    LStatus Open(const LOptions& loptions, const std::string& dbpath);
    void SetSchema(Schema& schema);
};

} // end namespace lightgraph