#include <cstring>
#include <distwt/mpi/uint64_pack.hpp>

// impl for strings of length 8
using str8 = unsigned char[8];
using str8_pack_t = uint64_pack_t<str8, 8>;

union str8_u64 {
    str8 str;
    uint64_t u64;
};

template<>
uint64_t str8_pack_t::pack_uint64(const str8& items) {
    str8_u64 pack;
    memcpy(pack.str, items, 8);
    return pack.u64;
}

template<>
void str8_pack_t::unpack_uint64(uint64_t u64, str8& items) {
    str8_u64 pack;
    pack.u64 = u64;
    memcpy(items, pack.str, 8);
}
