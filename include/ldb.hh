//
// Created by tramboo on 2020/2/24.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

namespace lightgraph
{

typedef rocksdb::Status LStatus;
typedef rocksdb::Options LOptions;

class LDB {
    rocksdb::DB* db;
    public:
    LDB(): db(nullptr) {}
    // No copying allowed
    LDB(const LDB&) = delete;
    void operator=(const LDB&) = delete;

    virtual ~LDB() {
        delete db;
    }

    LStatus Open(const LOptions& loptions, const std::string& name);
};

} // end namespace lightgraph