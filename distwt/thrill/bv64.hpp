#pragma once

#include <distwt/common/bv64.hpp>
#include <thrill/data/serialization_fwd.hpp>

// serialization for 64-bit bit vectors
template<typename Archive>
struct thrill::data::Serialization<Archive, bv64_t>{
    static void Serialize(const bv64_t& x, Archive& ar) {
        ar.PutRaw(x.to_ullong());
    }
    static bv64_t Deserialize(Archive& ar) {
        return bv64_t(ar.template GetRaw<uint64_t>());
    }
    static constexpr bool   is_fixed_size = true;
    static constexpr size_t fixed_size    = sizeof(uint64_t);
};
