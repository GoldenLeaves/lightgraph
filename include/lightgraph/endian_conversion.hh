//
// Created by tramboo on 2020/4/3.
//

#include <netinet/in.h>
#include <cstdint>

namespace lightgraph {

class EndianConversion {
public:
    static __uint16_t NtoH16(__uint16_t val16);
    static __uint32_t NtoH32(__uint32_t val32);
    static __uint64_t NtoH64(__uint64_t val64);

    static __uint16_t HtoN16(__uint16_t val16);
    static __uint32_t HtoN32(__uint32_t val32);
    static __uint64_t HtoN64(__uint64_t val64);
};

} // end namespace lightgraph
