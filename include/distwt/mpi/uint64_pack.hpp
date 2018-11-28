#pragma once

#include <tlx/math/div_ceil.hpp>

template<typename items_t, size_t N>
class uint64_pack_t {
private:
    static_assert(sizeof(items_t) == 8, "items_t must be 64 bits in size.");

    union pack_t {
        uint64_t packed;
        items_t items;
    };

public:
    static inline size_t required_bufsize(const size_t num_items) {
        return tlx::div_ceil(num_items, N);
    }

    template<typename src_t>
    static void pack(
        const src_t& src,
        const size_t src_offs,
        uint64_t* dst,
        const size_t num) {

        pack_t buf;
        size_t count = 0;

        // write items into buffer
        for(size_t i = 0; i < num; i++) {
            buf.items[count++] = src[src_offs + i];
            if(count >= N) {
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

    template<typename dst_t>
    static void unpack(
        const uint64_t* src,
        dst_t& dst,
        const size_t dst_offs,
        const size_t num) {

        pack_t buf;
        size_t count = N;

        // read items into buffer
        for(size_t i = 0; i < num; i++) {
            if(count >= N) {
                buf.packed = *src++;
                count = 0;
            }

            dst[dst_offs + i] = buf.items[count++];
        }
    }
};
