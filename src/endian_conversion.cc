//
// Created by tramboo on 2020/4/3.
//

#include "endian_conversion.hh"

namespace lightgraph {

__uint16_t EndianConversion::NtoH16(__uint16_t val16) {
    return ntohs(val16);
}

__uint32_t EndianConversion::NtoH32(__uint32_t val32) {
    return ntohl(val32);
}

__uint64_t EndianConversion::NtoH64(__uint64_t val64)
{
    __uint64_t res64 = 0;
    if (__BYTE_ORDER == __LITTLE_ENDIAN)
    {
        auto low = (__uint32_t) (val64 & (__uint64_t)0x00000000FFFFFFFFLL);
        auto high = (__uint32_t) ((val64 & (__uint64_t)0xFFFFFFFF00000000LL) >> (__uint32_t)32);
        low = ntohl(low);
        high = ntohl(high);
        res64 = (__uint64_t)high + (((__uint64_t) low) << (__uint32_t)32);
    }
    else if (__BYTE_ORDER == __BIG_ENDIAN)
    {
        res64 = val64;
    }
    return res64;
}

__uint16_t EndianConversion::HtoN16(__uint16_t val16) {
    return htons(val16);
}

__uint32_t EndianConversion::HtoN32(__uint32_t val32) {
    return htonl(val32);
}

__uint64_t EndianConversion::HtoN64(__uint64_t val64)
{
    __uint64_t res64 = 0;
    if (__BYTE_ORDER == __LITTLE_ENDIAN)
    {
        auto low = (__uint32_t) (val64 & (__uint64_t)0x00000000FFFFFFFFLL);
        auto high = (__uint32_t) ((val64 & (__uint64_t)0xFFFFFFFF00000000LL) >> (__uint32_t)32);
        low = htonl(low);
        high = htonl(high);
        res64 = (__uint64_t)high + (((__uint64_t) low) << (__uint32_t)32);
    }
    else if (__BYTE_ORDER == __BIG_ENDIAN)
    {
        res64 = val64;
    }
    return res64;
}

} // end namespace lightgraph