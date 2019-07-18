#include <distwt/mpi/uint64_pack_bv64.hpp>

template<>
uint64_t bv64_pack_t::pack_uint64(const bv64_t& items) {
    return items.to_ullong();
}

template<>
void bv64_pack_t::unpack_uint64(uint64_t u64, bv64_t& items) {
    items = bv64_t(u64);
}
