//
// Created by tramboo on 2020/3/15.
//

#include "properties.hh"

namespace lightgraph {

Properties& Properties::operator=(const Properties& other)
{
    if(this != &other) {
        _root = other._root;
    }
    return *this;
}

Properties Properties::CreatePropertiesFrom(const std::string &json_str)
{
    Json::CharReaderBuilder builder;
    JSONCPP_STRING errs;
    Json::CharReader* reader = builder.newCharReader();
    Json::Value root;
    if (!reader->parse(json_str.data(), json_str.data() + json_str.size(), &root, &errs)) {
        return {};
    }
    return Properties(root);
}

bool Properties::LoadFrom(const std::string &json_str)
{
    Json::Reader reader;
    _root.clear();
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
    Json::Value a;
    return _root.toStyledString();
}

std::string Properties::GetValueBy(const std::string &key)
{
    return _root[key].isNull()? "" : _root[key].asString();
}

} // end namespace lightgraph