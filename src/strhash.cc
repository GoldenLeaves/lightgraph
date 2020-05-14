//
// Created by tramboo on 2020/3/23.
//

#include "lightgraph/strhash.hh"
#include <cstring>

namespace lightgraph
{

static inline
unsigned int MurmurHash2(const void* key, unsigned int len, unsigned int seed)
{
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const unsigned int m = 0x5bd1e995;
    const unsigned int r = 24;

    // Initialize the hash to a 'random' value

    unsigned int h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const auto * data = (const unsigned char *)key;

    while(len >= 4)
    {
        unsigned int k = *(unsigned int*)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch(len)
    {
    case 3: h ^= (unsigned)data[2] << (unsigned)16;
    case 2: h ^= (unsigned)data[1] << (unsigned)8;
    case 1: h ^= data[0];
            h *= m;
    default: break;
    }

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> (unsigned)13;
    h *= m;
    h ^= h >> (unsigned)15;

    return h;
}

size_t StrHashFunc::operator()(const std::string& ctx) const
{
    return MurmurHash2(ctx.data(), ctx.size(), static_cast<size_t>(0xc70f6907UL));
}

bool StrEqualFunc::operator()(const std::string& a, const std::string& b) const
{
    return a == b;
}

bool StrCmpFunc::operator()(const std::string &a, const std::string &b) const
{
    return  std::strcmp(a.c_str(), b.c_str()) < 0;
}

}

