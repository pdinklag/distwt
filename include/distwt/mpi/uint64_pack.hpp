#pragma once

#include <tlx/math/div_ceil.hpp>

union str8_pack_t {
    static constexpr size_t num_items = 8;
    static inline size_t bufsize(const size_t items) {
        return tlx::div_ceil(items, 8);
    }

    unsigned char items[num_items];
    uint64_t packed;
};

template<typename U, typename src_t>
void pack_uint64(
    const src_t& src,
    const size_t src_offs,
    uint64_t* dst,
    const size_t num) {

    static_assert(sizeof(U) == 8, "Union type must be 64 bits in size.");

    U buf;
    size_t count = 0;

    // write items into buffer
    for(size_t i = 0; i < num; i++) {
        buf.items[count++] = src[src_offs + i];
        if(count >= U::num_items) {
            // buffer full, write to destination
            *dst++ = buf.packed;
            count = 0;
        }
    }

    // remainder
    if(count > 0) {
        *dst++ = buf.packed;
    }
}

template<typename U, typename dst_t>
void unpack_uint64(
    const uint64_t* src,
    dst_t& dst,
    const size_t dst_offs,
    const size_t num) {

    static_assert(sizeof(U) == 8, "Union type must be 64 bits in size.");

    U buf;
    size_t count = U::num_items;

    // read items into buffer
    for(size_t i = 0; i < num; i++) {
        if(count >= U::num_items) {
            buf.packed = *src++;
            count = 0;
        }

        dst[dst_offs + i] = buf.items[count++];
    }
}
