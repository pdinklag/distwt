#include <cassert>
#include <distwt/mpi/bit_vector.hpp>

#include <tlx/math/div_ceil.hpp>

size_t encode_bv_interval_msg(
    bv_interval_msg_t& msg,
    const bv_t& bv, size_t p, const size_t q,
    const size_t glob_first, const size_t glob_last) {

    assert(p <= q);
    assert(glob_first <= glob_last);
    assert(glob_last - glob_first == q - p);

    // allocate
    const size_t num_bits = (q-p)+1;
    msg.size = 2ULL + tlx::div_ceil(num_bits, 64ULL);
    assert(msg.size > 2ULL);

    msg.data = new uint64_t[msg.size];

    // message header
    msg.data[0] = glob_first;
    msg.data[1] = glob_last;

    // encode bits
    {
        std::bitset<64> bits;
        size_t i = 2, x = 0;
        while(p <= q) {
            bits[x++] = bv[p++];
            if(x >= 64ULL) {
                msg.data[i++] = bits.to_ullong();
                x = 0;
                bits.reset();
            }
        }

        // encode remaining bits
        if(x > 0) {
            msg.data[i] = bits.to_ullong();
        }
    }
    return num_bits;
}

size_t decode_bv_interval_msg(
    const bv_interval_msg_t& msg,
    bv_t& bv, const size_t glob_offs) {

    const size_t glob_first = msg.data[0];
    const size_t glob_last = msg.data[1];

    assert(glob_first <= glob_last);
    assert(glob_first >= glob_offs);

    size_t p = glob_first - glob_offs;
    const size_t q = glob_last - glob_offs;
    const size_t num_bits = q - p + 1;

    // decode bits
    size_t i = 2, x = 0;
    std::bitset<64> bits = msg.data[i++];

    while(p <= q) {
        bv[p++] = bits[x++];
        if(x >= 64ULL) {
            bits = msg.data[i++];
            x = 0;
        }
    }
    return num_bits;
}
