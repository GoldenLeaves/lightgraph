//
// Created by tramboo on 2020/3/15.
//

#include "properties.hh"

namespace lightgraph {

Properties Properties::CreateProperties(const std::string &json_str)
{
    Json::Reader reader;
    Json::Value root;
    if(reader.parse(json_str, root)) {
        return Properties(root);
    }
    return Properties();
}

bool Properties::LoadFrom(const std::string &json_str)
{
    Json::Reader reader;
    return reader.parse(json_str, _root);
}

bool Properties::AddProperty(const std::string &key, const std::string& value)
{
    if(key.empty()) return false;
    _root[key] = value;
    return true;
}

bool Properties::AppendProperty(const std::string &key, const std::string &value)
{
    if(key.empty()) return false;
    _root[key].append(value);
    return true;
}

bool Properties::AddArrayProperty(const std::string &key, const std::vector<std::string> &values)
{
    if(key.empty()) return false;
    for(auto& v: values) {
        _root[key].append(v);
    }
    return true;
}

std::string Properties::AsString() const
{
    return _root.asString();
}

std::string Properties::GetValueBy(const std::string &key)
{
    return _root[key].isNull()? "" : _root[key].asString();
}

} // end namespace lightgraph