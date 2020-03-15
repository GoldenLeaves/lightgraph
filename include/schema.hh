//
// Created by tramboo on 2020/3/13.
//

#include <string>
#include <vector>
#include <set>
#include <unordered_map>

namespace lightgraph
{

// typedef of vertex label and edge label.
typedef std::string VLabel;
typedef std::string Elabel;

//class ELabel {
//    std::string from_label;
//    std::string inner_label;
//    std::string to_label;
//public:
//    ELabel() = default;
//    ELabel(std::string& from, std::string& inner, std::string& to)
//            : from_label(from), inner_label(inner), to_label(to) {}
//    ELabel(const char* from, const char* inner, const char* to)
//            : from_label(from), inner_label(inner), to_label(to) {}
//    ~ELabel() = default;
//};

// The label schema of edges.
class Schema {
    std::set<std::string> _label_set;
public:
    Schema() = default;
    ~Schema() = default;

    void AddEdgeLabel(std::string& label);
    void AddEdgeLabel(const char* label);
    void AddEdgeLabels(std::vector<std::string>& label_lists);

    std::unordered_map<std::string, int>&& InnerCoding();
};

} // end namespace lightgraph
