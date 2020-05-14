//
// Created by tramboo on 2020/3/24.
//

#include <lightgraph/ldb.hh>
#include <iostream>

std::string DBPath = "../data/vertex_test";

lightgraph::Schema GetSchema() {
    lightgraph::Schema schema;
    schema.AddEdgeLabel("knows");
    schema.AddEdgeLabel("likes");
    return schema;
}

lightgraph::Properties GetProperties1() {
    lightgraph::Properties props;
    props.AddProperty("label", "person");
    return props;
}

lightgraph::Properties GetProperties2() {
    lightgraph::Properties props;
    props.AddProperty("label", "comments");
    return props;
}

int main() {
    std::cout << "Test vertex" << std::endl;
    lightgraph::LDB db(GetSchema());
    lightgraph::LOptions options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // Open DB
    auto s = db.Open(options, DBPath);
    assert(s.ok());
    std::cout << "Open DB successfully." << std::endl;

    // Put vertices
    //std::cout << GetProperties1().AsString() << std::endl;
    s = db.VertexPut(111, GetProperties1());
    assert(s.ok());
    s = db.VertexPut(222, GetProperties1());
    assert(s.ok());

    std::cout << "Put prop1: " << GetProperties1().AsString() << std::endl;

    // WriteBatch vertices
    std::vector<std::pair<lightgraph::__vertex_id_t, lightgraph::Properties> > vertices;
    vertices.emplace_back(3333, GetProperties2());
    vertices.emplace_back(4444, GetProperties2());
    vertices.emplace_back(5555, GetProperties2());
    s = db.VertexWriteBatch(vertices);
    assert(s.ok());

    std::cout << "Put prop2: " << GetProperties2().AsString() << std::endl;

    std::cout << "Put vertices successfully." << std::endl;

    // Read vertices
    lightgraph::Properties prop;
    std::string value;
    s = db.VertexGet(1234, &value);
    assert(s.IsNotFound());
    s = db.VertexGet(111, &value);
    assert(s.ok());
    std::cout << "Got prop1: " << value << std::endl;
    prop = lightgraph::Properties::CreatePropertiesFrom(value);
    std::cout << "Label: " << prop.GetValueBy("label") << std::endl;
    s = db.VertexGet(5555, &value);
    assert(s.ok());
    std::cout << "Got prop1: " << value << std::endl;
    prop = lightgraph::Properties::CreatePropertiesFrom(value);
    std::cout << "Label: " << prop.GetValueBy("label") << std::endl;

    std::cout << "Get values successfully." << std::endl;

    return 0;
 }