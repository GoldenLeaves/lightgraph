//
// Created by tramboo on 2020/3/15.
//

#include "schema.hh"

namespace lightgraph
{

void Schema::AddEdgeLabel(std::string &label) {
    _label_set.insert(label);
}

void Schema::AddEdgeLabel(const char* label) {
    _label_set.insert(label);
}

void Schema::AddEdgeLabels(std::vector<std::string> &label_lists) {
    for(auto& label: label_lists) {
        _label_set.insert(label);
    }
}

std::unordered_map<std::string, int>&& Schema::InnerCoding() {
    std::unordered_map<std::string, int> inner_code_map;
    int code = 0;
    for(auto& label: _label_set) {
        inner_code_map.insert(std::make_pair(label, code++));
    }
    return std::move(inner_code_map);
}

} // end namespace lightgraph