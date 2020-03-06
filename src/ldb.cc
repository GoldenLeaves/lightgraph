//
// Created by tramboo on 2020/3/6.
//

#include "ldb.hh"

namespace lightgraph
{

LStatus LDB::Open(const LOptions& loptions, const std::string& name) {
    return rocksdb::DB::Open(loptions, name, &db);
}

} // end namespace lightgraph


