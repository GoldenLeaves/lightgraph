//
// Created by tramboo on 2020/3/15.
//

#include "schema.hh"

namespace lightgraph
{

void Schema::AddEdgeLabel(std::string &label)
{
    _label_set.insert(label);
}

void Schema::AddEdgeLabel(const char* label)
{
    _label_set.insert(label);
}

void Schema::AddEdgeLabels(std::vector<std::string> &label_lists)
{
    for(auto& label: label_lists) {
        _label_set.insert(label);
    }
}

void Schema::InnerCoding(InnerMap& inner_map) const
{
    __label_id_t code = 0;
    for(auto& label: _label_set) {
        inner_map.label_to_id.insert(std::make_pair(label, code));
        inner_map.id_to_label.insert(std::make_pair(code, label));
        code++;
    }
}

} // end namespace lightgraph