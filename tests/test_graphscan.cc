//
// Created by tramboo on 2020/4/1.
//

#include <ldb.hh>
#include <iostream>

std::string DBPath = "../data/graphscan_test";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

void AddDeltas(lightgraph::LDB& db) {
    lightgraph::LStatus s;
    // Put deltas
    s = db.DeltaPut(
            {1001, "knows", 2001, 2, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1001, "knows", 2001, 10, lightgraph::OpFlag::Delete},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1001, "knows", 2001, 18, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1001, "knows", 2001, 22, lightgraph::OpFlag::Delete},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1001, "knows", 2002, 6, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1001, "knows", 2003, 4, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1001, "knows", 2003, 9, lightgraph::OpFlag::Delete},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1005, "likes", 3001, 6, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1005, "likes", 3001, 11, lightgraph::OpFlag::Delete},
            lightgraph::Properties());
    assert(s.ok());
    s = db.DeltaPut(
            {1005, "likes", 3001, 24, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1008, "knows", 2004, 9, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1009, "likes", 3007, 5, lightgraph::OpFlag::Insert},
            lightgraph::Properties());
    assert(s.ok());

    s = db.DeltaPut(
            {1009, "likes", 3007, 7, lightgraph::OpFlag::Delete},
            lightgraph::Properties());
    assert(s.ok());
}

void TestDeltaScan(lightgraph::LDB& db) {
    auto iter = db.DeltaScan(5, 20);
    lightgraph::GraphDelta delta;
    while(iter->GetNext(db, &delta)) {
        std::cout << "GraphDelta: "
                  << delta.edge.src << ", "
                  << delta.edge.label << ", "
                  << delta.edge.dst << ", "
                  << delta.t << ", "
                  << ((delta.op == lightgraph::OpFlag::Insert)? "Insert" : "Delete") << std::endl;
    }
}

void TestDeltaScanOfOutV(lightgraph::LDB& db) {
    auto iter = db.DeltaScanOfOutV(1001, "knows", 5, 20);
    lightgraph::GraphDelta delta;
        while(iter->GetNext(db, &delta)) {
        std::cout << "GraphDelta: "
                  << delta.edge.src << ", "
                  << delta.edge.label << ", "
                  << delta.edge.dst << ", "
                  << delta.t << ", "
                  << ((delta.op == lightgraph::OpFlag::Insert)? "Insert" : "Delete") << std::endl;
    }
}

void TestDeltaScanOfEdge(lightgraph::LDB& db) {
    auto iter = db.DeltaScanOfEdge({1001, "knows", 2001}, 4);
    lightgraph::GraphDelta delta;
        while(iter->GetNext(db, &delta)) {
        std::cout << "GraphDelta: "
                  << delta.edge.src << ", "
                  << delta.edge.label << ", "
                  << delta.edge.dst << ", "
                  << delta.t << ", "
                  << ((delta.op == lightgraph::OpFlag::Insert)? "Insert" : "Delete") << std::endl;
    }
}

void TestGetOutVDuring(lightgraph::LDB& db) {
    std::cout << "Test GetOutVDuring" << std::endl;
    auto iter = db.GetOutVDuring(1001, "knows", 1, 5);
    lightgraph::__vertex_id_t dst;
    while(iter->GetNext(dst)) {
        std::cout << "src: " << 1001 << ", " << "dst: " << dst << std::endl;
    }
}

void TestGetOutVAt(lightgraph::LDB& db) {
    auto iter = db.GetOutVAt(1001, "knows", 5);
    lightgraph::__vertex_id_t dst;
    while(iter->GetNext(dst)) {
        std::cout << "src: " << 1001 << ", " << "dst: " << dst << std::endl;
    }
}

void TestEdgeExists(lightgraph::LDB& db,
        const lightgraph::GraphEdge& edge, lightgraph::__time_t time) {
    std::cout << "Edge: " << edge.src << " " << edge.label << " " << edge.dst << std::endl;
    std::cout << "Time: " << time << std::endl;
    std::cout << "Status: " << (db.EdgeExists(edge, time)? "Exists" : "Not Exists") << std::endl;
}

int main() {
    std::cout << "Test Graph Scan" << std::endl;
    lightgraph::LDB db(GetSchema());
    lightgraph::LOptions options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    //options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(3));

    // Open DB
    auto s = db.Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    db.UsingIndex();
    //AddDeltas(db);
    //TestDeltaScan(db);
    //TestDeltaScanOfOutV(db);
    //TestDeltaScanOfEdge(db);
    //TestGetOutVDuring(db);
    TestGetOutVAt(db);
    //TestEdgeExists(db, {1005, "likes", 3001}, 25);

    return 0;
}