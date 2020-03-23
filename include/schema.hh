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
typedef std::unordered_map<std::string, __label_id_t, StrHashFunc, StrEqualFunc> LabelIdMap;

// The label schema of edges.
class Schema {
    std::set<std::string> _label_set;
public:
    Schema() = default;
    ~Schema() = default;
    void AddEdgeLabel(std::string& label);
    void AddEdgeLabel(const char* label);
    void AddEdgeLabels(std::vector<std::string>& label_lists);
private:
    LabelIdMap&& InnerCoding() const;
    friend class LDB;
};

} // end namespace lightgraph
