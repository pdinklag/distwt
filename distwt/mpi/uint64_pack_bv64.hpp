#include <distwt/common/bv64.hpp>
#include <distwt/mpi/uint64_pack.hpp>

// impl for bit vectors of length 64
using bv64_pack_t = uint64_pack_t<bv64_t, 64>;

template<> uint64_t bv64_pack_t::pack_uint64(const bv64_t& items);
template<> void bv64_pack_t::unpack_uint64(uint64_t u64, bv64_t& items);
