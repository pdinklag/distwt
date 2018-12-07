#include <bitset>
#include <distwt/mpi/uint64_pack.hpp>

// impl for bit vectors of length 64
using bv64 = std::bitset<64>;
using bv64_pack_t = uint64_pack_t<bv64, 64>;

template<>
uint64_t bv64_pack_t::pack_uint64(const bv64& items) {
    return items.to_ullong();
}

template<>
void bv64_pack_t::unpack_uint64(uint64_t u64, bv64& items) {
    items = bv64(u64);
}
