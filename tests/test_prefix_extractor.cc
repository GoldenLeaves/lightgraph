//
// Created by tramboo on 2020/4/2.
//

#include <ldb.hh>
#include <iostream>
#include <chrono>

std::string DBPath = "../data/prefix_extractor_test";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

void AddDeltas(lightgraph::LDB& db) {
    lightgraph::LStatus s;
    for(lightgraph::__vertex_id_t u = 0; u < 1000; u++) {
        for(lightgraph::__vertex_id_t v = 0; v < 10; v++) {
            for(lightgraph::__time_t t = 0; t < 1000; t += 5) {
                s = db.DeltaPut(
                        {u, "knows", v, t, lightgraph::OpFlag::Insert},
                        lightgraph::Properties());
                assert(s.ok());
                t += 5;
                s = db.DeltaPut(
                        {u, "knows", v, t, lightgraph::OpFlag::Delete},
                        lightgraph::Properties());
                assert(s.ok());
            }
        }
    }
}

int main() {
    std::cout << "Test Graph Scan" << std::endl;
    lightgraph::LDB db(GetSchema());
    lightgraph::LOptions options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
//    options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(20));
//    options.memtable_prefix_bloom_size_ratio = 0.001;
//    // Enable prefix bloom for SST files
//    rocksdb::BlockBasedTableOptions table_options = rocksdb::BlockBasedTableOptions();
//    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
//    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // Open DB
    auto s = db.Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    db.UsingIndex();

    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

//    time_start = std::chrono::steady_clock::now();
//    AddDeltas(db);
//    time_end = std::chrono::steady_clock::now();
//    time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
//    std::cout << "Put Time Used: " << time_used.count() << std::endl;

    time_start = std::chrono::steady_clock::now();
    auto iter = db.DeltaScanOfOutV(345, "knows");
    lightgraph::GraphDelta delta;
    while(iter->GetNext(db, &delta)){}
    time_end = std::chrono::steady_clock::now();
    time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
    std::cout << "Scan Time Used: " << time_used.count() << std::endl;

    return 0;
}