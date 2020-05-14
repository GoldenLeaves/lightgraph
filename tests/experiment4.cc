//
// Created by tramboo on 2020/4/4.
//

#include <lightgraph/ldb.hh>
#include <iostream>
#include <chrono>
#include <ctime>
#include <random>

std::string DBPath = "../data/experiment4";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

void AddDeltas(lightgraph::LDB& db) {
    db.UsingIndex();

    lightgraph::Properties props;
    lightgraph::__time_t t;
    lightgraph::LStatus s;
    for(lightgraph::__vertex_id_t src = 0; src < 100; src++) {
        for(lightgraph::__vertex_id_t dst = 0; dst < 200; dst++) {
            for(int i = 0; i < 1000;) {
                t = (5 * i) + (random() % 5) + 1;
                s = db.DeltaPut({src, "likes", dst, t, lightgraph::OpFlag::Insert}, props);
                assert(s.ok());
                i++;
                t = (5 * i) + (random() % 5) + 1;
                s = db.DeltaPut({src, "likes", dst, t, lightgraph::OpFlag::Delete}, props);
                assert(s.ok());
                i++;
            }
        }
    }
}

void TestGetOutVDuringWithIndex(lightgraph::LDB& db) {
    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

    db.UsingIndex();

    std::cout << "Test GetOutVDuring With Index" << std::endl;

    lightgraph::__vertex_id_t dst;
    for(lightgraph::__time_t upper = 1000; upper <= 5000; upper += 1000) {
        time_start = std::chrono::steady_clock::now();
        for(lightgraph::__vertex_id_t src = 0; src < 100; src++) {
            auto iter = db.GetOutVDuring(src, "likes", 0, upper);
            while(iter->GetNext(dst)) {}
        }
        time_end = std::chrono::steady_clock::now();
        time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
        std::cout << "Time Used: " << time_used.count() << std::endl;
    }
}

void TestGetOutVDuringWithoutIndex(lightgraph::LDB& db) {
    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

    db.ClosingIndex();

    std::cout << "Test GetOutVDuring Without Index" << std::endl;

    lightgraph::__vertex_id_t dst;
    for(lightgraph::__time_t upper = 1000; upper <= 5000; upper += 1000) {
        time_start = std::chrono::steady_clock::now();
        for(lightgraph::__vertex_id_t src = 0; src < 100; src++) {
            auto iter = db.GetOutVDuring(src, "likes", 0, upper);
            while(iter->GetNext(dst)) {}
        }
        time_end = std::chrono::steady_clock::now();
        time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
        std::cout << "Time Used: " << time_used.count() << std::endl;
    }
}

void TestGetOutVAtWithIndex(lightgraph::LDB& db) {
    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

    db.UsingIndex();

    std::cout << "Test GetOutVAt With Index" << std::endl;
    lightgraph::__vertex_id_t dst;
    for(lightgraph::__time_t t = 1000; t <= 5000; t += 1000) {
        time_start = std::chrono::steady_clock::now();
        for(lightgraph::__vertex_id_t src = 0; src < 100; src++) {
            auto iter = db.GetOutVAt(src, "likes", t);
            while(iter->GetNext(dst)) {}
        }
        time_end = std::chrono::steady_clock::now();
        time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
        std::cout << "Time Used: " << time_used.count() << std::endl;
    }

}

void TestGetOutVAtWithOutIndex(lightgraph::LDB& db) {
    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

    db.ClosingIndex();

    std::cout << "Test GetOutVAt Without Index" << std::endl;
    lightgraph::__vertex_id_t dst;
    for(lightgraph::__time_t t = 1000; t <= 5000; t += 1000) {
        time_start = std::chrono::steady_clock::now();
        for(lightgraph::__vertex_id_t src = 0; src < 100; src++) {
            auto iter = db.GetOutVAt(src, "likes", t);
            while(iter->GetNext(dst)) {}
        }
        time_end = std::chrono::steady_clock::now();
        time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
        std::cout << "Time Used: " << time_used.count() << std::endl;
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
    //options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(3));

    // Open DB
    auto s = db.Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    unsigned seed;  // Random generator seed
    // Use the time function to get a "seed” value for srand
    seed = time(nullptr);
    srand(seed);

//    AddDeltas(db);
    TestGetOutVDuringWithIndex(db);
    TestGetOutVDuringWithoutIndex(db);
    TestGetOutVAtWithIndex(db);
    TestGetOutVAtWithOutIndex(db);

    return 0;
}
