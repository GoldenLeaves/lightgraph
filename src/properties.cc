//
// Created by tramboo on 2020/3/15.
//

#include "properties.hh"

namespace lightgraph {

Properties Properties::CreateProperties(std::string &json_str) {
    Json::Reader reader;
    Json::Value root;
    reader.parse(json_str, root);
    return Properties(root);
}

void Properties::AddProperty(const std::string &key, const std::string& value) {
    _root[key] = value;
}

void Properties::AppendProperty(const std::string &key, const std::string &value) {
    _root[key].append(value);
}

void Properties::AddArrayProperty(const std::string &key, const std::vector<std::string> &values) {
    for(auto& v: values) {
        _root[key].append(v);
    }
}

std::string Properties::AsString() {
    return _root.asString();
}

std::string Properties::GetValueBy(const std::string &key) {
    return _root[key].isNull()? "" : _root[key].asString();
}

} // end namespace lightgraph