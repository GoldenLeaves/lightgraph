//
// Created by tramboo on 2020/3/15.
//

#include <jsoncpp/json/json.h>

namespace lightgraph
{

class Properties {
    Json::Value _root;
public:
    Properties() = default;
    explicit Properties(Json::Value& value): _root(value) {}
    ~Properties() = default;

    // Parse a json string and create a Properties class.
    static Properties CreateProperties(std::string& json_str);

    // Add a new key/value property.
    // If the added key is existed, the new value will overwrite the old value.
    void AddProperty(const std::string& key, const std::string& value);
    // Append a new key/value property.
    // If the added key is existed, this property will be converted to a array property
    //  and the new value will be appended to the array of this property.
    // If the added key is not existed, this function will add a new key/value_array property directly
    //  and set the first array member to the new value.
    void AppendProperty(const std::string& key, const std::string& value);
    // Append a new key/value_array property.
    // If the added key is existed, the new value array will be appended to the old one.
    void AddArrayProperty(const std::string& key, const std::vector<std::string>& values);

    std::string AsString();

    std::string GetValueBy(const std::string& key);
};

} // end namespace lightgraph