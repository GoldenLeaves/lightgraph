//
// Created by tramboo on 2020/3/31.
//

#include <ldb.hh>
#include <iostream>

std::string DBPath = "../data/iter_test";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

int main() {
    std::cout << "Test vertex" << std::endl;
    lightgraph::LDB db(GetSchema());
    lightgraph::LOptions options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(3));

    // Open DB
    auto s = db.Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    // Put vertices
    s = db.VertexPut(1111, lightgraph::Properties());
    assert(s.ok());
    s = db.VertexPut(1112, lightgraph::Properties());
    assert(s.ok());
    s = db.VertexPut(1113, lightgraph::Properties());
    assert(s.ok());
    s = db.VertexPut(1115, lightgraph::Properties());
    assert(s.ok());
    s = db.VertexPut(1117, lightgraph::Properties());
    assert(s.ok());

    auto iter = db.VertexScan(lightgraph::LReadOptions());
    lightgraph::__vertex_id_t v;
    while(iter->GetNext(v)) {
        std::cout << "Get vertex: " << v << std::endl;
    }

    return 0;
 }