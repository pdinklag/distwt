#pragma once

#include <bitset>
#include <vector>

using bv_t = std::vector<bool>;

struct bv_interval_msg_t {
    uint64_t* data;
    size_t size;

    inline bv_interval_msg_t() : data(nullptr), size(0) {
    }

    inline bv_interval_msg_t(const size_t _size)
        : data(new uint64_t[_size]), size(_size) {
    }

    inline ~bv_interval_msg_t() {
        if(data) {
            delete[] data;
            data = nullptr;
        }
    }

    inline operator bool() const {
        return (data);
    }
};

size_t encode_bv_interval_msg(
    bv_interval_msg_t& msg,
    const bv_t& bv, size_t p, const size_t q,
    const size_t glob_first, const size_t glob_last);

size_t decode_bv_interval_msg(
    const bv_interval_msg_t& msg,
    bv_t& bv, const size_t glob_offs);
