#include <distwt/common/bitrev.hpp>

// from bit twiddling hacks
const uint8_t bitrev8[256] = 
{
#   define R2(n)     n,     n + 2*64,     n + 1*64,     n + 3*64
#   define R4(n) R2(n), R2(n + 2*16), R2(n + 1*16), R2(n + 3*16)
#   define R6(n) R4(n), R4(n + 2*4 ), R4(n + 1*4 ), R4(n + 3*4 )
    R6(0), R6(2), R6(1), R6(3)
};

// from bit twiddling hacks
uint32_t bitrev32(uint32_t v) {
    uint32_t c; // c will get v reversed

    uint8_t* p = (uint8_t*) &v;
    uint8_t* q = (uint8_t*) &c;
    q[3] = bitrev8[p[0]]; 
    q[2] = bitrev8[p[1]]; 
    q[1] = bitrev8[p[2]]; 
    q[0] = bitrev8[p[3]];

    return c;
}

// custom
uint32_t bitrev(uint32_t x, uint8_t b) {
    if(!b) return 0;
    return bitrev32(x) >> (uint8_t(32) - b);
}
