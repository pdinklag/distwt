#include <distwt/mpi/uint64_pack_bv64.hpp>
#include <distwt/mpi/uint64_pack_str8.hpp>

template<>
uint64_t bv64_pack_t::pack_uint64(const bv64_t& items) {
    return items.to_ullong();
}

template<>
void bv64_pack_t::unpack_uint64(uint64_t u64, bv64_t& items) {
    items = bv64_t(u64);
}

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

