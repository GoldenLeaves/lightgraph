//
// Created by tramboo on 2020/3/6.
//

#include "ldb.hh"

namespace lightgraph
{

LDB::~LDB() {
    delete _db;
}

LStatus LDB::Open(const LOptions& loptions, const std::string& dbpath) {
    return rocksdb::DB::Open(loptions, dbpath, &_db);
}

void LDB::SetSchema(Schema &schema) {
    _label_id_map = schema.InnerCoding();
}

} // end namespace lightgraph


