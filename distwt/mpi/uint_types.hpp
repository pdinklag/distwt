#pragma once

#include <distwt/mpi/thrill_common_uint_types.hpp>

using uint40_t = thrill::common::uint40;
namespace std {
    template<> struct hash<uint40_t> {
        size_t operator()(const uint40_t& x) const {
            size_t hi  = std::hash<uint8_t>{}(x >> 32ULL);
            size_t lo = std::hash<uint32_t>{}(x & 0xFFFFFFFFULL);

            // based on Boost's hash_combine
            return hi + 0x9e3779b9 + (lo << 6) + (lo >> 2);
        }
    };
}
