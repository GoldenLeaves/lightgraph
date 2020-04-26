//
// Created by tramboo on 2020/4/26.
//

#include <ldb.hh>
#include <iostream>
#include <chrono>
#include <ctime>
#include <random>
#include <algorithm>
#include <cstdlib>
#include <thread>

std::string DBPath = "../data/experiment5";
int arrayNum = 5000000;
int threadNum = 3;
//std::vector<lightgraph::__vertex_id_t> vArray;
//std::vector<lightgraph::GraphDelta> eArray;
lightgraph::LDB* db;

//void InsertVertex(int id) {
//    std::chrono::steady_clock::time_point time_start;
//    std::chrono::steady_clock::time_point time_end;
//    std::chrono::duration<double> time_used{};
//    lightgraph::Properties props;
//    lightgraph::LStatus s;
//    int loc = id;
//    int num = (arrayNum / threadNum);
//    int step = threadNum;
//    time_start = std::chrono::steady_clock::now();
//    for(int  i = 0; i < num; i++) {
//        s = db->VertexPut(static_cast<lightgraph::__vertex_id_t>(vArray[loc]), props);
//        assert(s.ok());
//        loc += step;
//    }
//    time_end = std::chrono::steady_clock::now();
//    time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
//    std::cout << "Thread " << id << "finished!    " << "Time Used: " << time_used.count() << std::endl;
//}

void InsertEdge(int id) {
    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};
    lightgraph::Properties props;
    lightgraph::LStatus s;
    int num = (arrayNum / threadNum);

    unsigned seed;  // Random generator seed
    // Use the time function to get a "seed” value for srand
    seed = time(nullptr);
    srand(seed);
    std::vector<lightgraph::GraphDelta> eArray;
    eArray.reserve(num);
    for(lightgraph::__time_t i = id; i < threadNum * num; i += threadNum) {
        eArray.emplace_back(random() % 1000, "likes", random() % 1000, i, lightgraph::OpFlag::Null);
    }

    time_start = std::chrono::steady_clock::now();
    for(auto& delta: eArray) {
        s = db->DeltaPut(delta, props);
        assert(s.ok());
    }
    time_end = std::chrono::steady_clock::now();
    time_used = std::chrono::duration_cast<std::chrono::duration<double>>(time_end - time_start);
    std::cout << "Thread " << id << "finished!    " << "Time Used: " << time_used.count() << std::endl;
}

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

int main() {
    db = new lightgraph::LDB(GetSchema());
    lightgraph::LOptions options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // Open DB
    auto s = db->Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    std::chrono::steady_clock::time_point time_start;
    std::chrono::steady_clock::time_point time_end;
    std::chrono::duration<double> time_used{};

//    unsigned seed;  // Random generator seed
//    // Use the time function to get a "seed” value for srand
//    seed = time(nullptr);
//    srand(seed);
//    // Init vArray
//    vArray.reserve(arrayNum);
//    for(int id = 0; id < arrayNum; id++) {
//        vArray[id] = static_cast<lightgraph::__vertex_id_t >(id);
//    }
//    unsigned shuffle_seed = std::chrono::system_clock::now().time_since_epoch().count();
//    std::shuffle (vArray.begin(), vArray.end(), std::default_random_engine(shuffle_seed));
//    // Init eArray
//    eArray.reserve(arrayNum);
//    for(lightgraph::__time_t i = 0; i < arrayNum; i++) {
//        eArray.emplace_back(random() % 1000, "likes", random() % 1000, i, lightgraph::OpFlag::Null);
//    }

    std::vector<std::thread> threads(threadNum);

    std::cout << "Begin Insert" << std::endl;

    for(int i = 0; i < threadNum; i++) {
        threads.emplace_back(InsertEdge, i);
        //threads[i].join();
        std::cout << "Thread " << i << " created!" << std::endl;
    }
//    for(int i = 0; i < threadNum; i++) {
//        if(threads[i].joinable()) {
//            threads[i].join();
//        }
//    }

    int a;
    std::cin >> a;
    return 0;
}