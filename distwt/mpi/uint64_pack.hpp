#pragma once

#include <tlx/math/div_ceil.hpp>

template<typename items_t, size_t N>
class uint64_pack_t {
private:
    static_assert(sizeof(items_t) == 8, "items_t must be 64 bits in size.");

public:
    static inline size_t required_bufsize(const size_t num_items) {
        return tlx::div_ceil(num_items, N);
    }

    static uint64_t pack_uint64(const items_t& items);
    static void unpack_uint64(uint64_t u64, items_t& items);

    template<typename src_t>
    static void pack(
        const src_t& src,
        const size_t src_offs,
        uint64_t* dst,
        const size_t num) {

        items_t buf;
        size_t count = 0;

        // write items into buffer
        for(size_t i = 0; i < num; i++) {
            buf[count++] = src[src_offs + i];
            if(count >= N) {
                // buffer full, write to destination
                *dst++ = pack_uint64(buf);
                count = 0;
            }
        }

        // remainder
        if(count > 0) {
            *dst++ = pack_uint64(buf);
        }
    }

    template<typename dst_t>
    static void unpack(
        const uint64_t* src,
        dst_t& dst,
        const size_t dst_offs,
        const size_t num) {

        items_t buf;
        size_t count = N;

        // read items into buffer
        for(size_t i = 0; i < num; i++) {
            if(count >= N) {
                unpack_uint64(*src++, buf);
                count = 0;
            }

            dst[dst_offs + i] = buf[count++];
        }
    }
};
