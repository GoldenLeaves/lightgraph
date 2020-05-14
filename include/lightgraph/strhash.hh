//
// Created by tramboo on 2020/3/23.
//

#include <unordered_map>
#include <string>

namespace lightgraph
{

//--------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby
// Note - This code makes a few assumptions about how your machine behaves -
// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4
// And it has a few limitations -
// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian machines.
static inline
unsigned int MurmurHash2(const void* key, unsigned int len, unsigned int seed);

struct StrHashFunc
{
    size_t operator()(const std::string& ctx) const;
};

struct StrEqualFunc
{
    bool operator()(const std::string& a, const std::string& b) const;
};

struct StrCmpFunc
{
    bool operator()(const std::string& a, const std::string& b) const;
};


} // end namespace lightgraph
