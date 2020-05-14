//
// Created by tramboo on 2020/3/13.
//

#include "typedef.hh"
#include "strhash.hh"
#include <string>
#include <vector>
#include <set>
#include <unordered_map>

namespace lightgraph
{

class LDB;

struct InnerMap {
    std::unordered_map<std::string, __label_id_t, StrHashFunc, StrEqualFunc> label_to_id;
    std::unordered_map<__label_id_t, std::string> id_to_label;

    InnerMap() = default;
    ~InnerMap() = default;
    InnerMap(const InnerMap&) = default;
    InnerMap& operator=(const InnerMap&) = default;
};

// The label schema of edges.
class Schema {
    std::set<std::string, StrCmpFunc> _label_set;
public:
    Schema() = default;
    ~Schema() = default;
    void AddEdgeLabel(std::string& label);
    void AddEdgeLabel(const char* label);
    void AddEdgeLabels(std::vector<std::string>& label_lists);
private:
    void InnerCoding(InnerMap& inner_map) const;
    friend class LDB;
};

} // end namespace lightgraph
