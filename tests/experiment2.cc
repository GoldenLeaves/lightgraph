//
// Created by tramboo on 2020/4/4.
//

#include <lightgraph/ldb.hh>
#include <iostream>
#include <chrono>
#include <ctime>
#include <random>

std::string DBPath = "../data/experiment2";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
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

    //db.UsingIndex();

    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

//    unsigned seed;  // Random generator seed
//    // Use the time function to get a "seed” value for srand
//    seed = time(nullptr);
//    srand(seed);
//
//    std::vector<lightgraph::GraphDelta> array;
//    array.reserve(10000000);
//    for(lightgraph::__time_t i = 0; i < 10000000; i++) {
//        array.emplace_back(random() % 10000, "likes", random() % 10000, i, lightgraph::OpFlag::Null);
//    }
//
//    //db.UsingIndex();
//
//    lightgraph::Properties props;
//    time_start = std::chrono::steady_clock::now();
//    for(auto& i : array) {
//        s = db.DeltaPut(i, props);
//        assert(s.ok());
//    }
//    time_end = std::chrono::steady_clock::now();
//    time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
//    std::cout << "Time Used: " << time_used.count() << std::endl;

    auto iter = db.DeltaScan();
    lightgraph::GraphDelta delta{};
    for(int x = 0; x < 5; x++) {
        time_start = std::chrono::steady_clock::now();
        for(int i = 0; i < 2000000; i++) {
            iter->GetNext(db, &delta);
        }
        time_end = std::chrono::steady_clock::now();
        time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
        std::cout << "Time Used: " << time_used.count() << std::endl;
    }

    return 0;
}