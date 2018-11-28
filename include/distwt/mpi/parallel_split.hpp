#pragma once

#include <array>
#include <cmath>
#include <cassert>
#include <functional>
#include <tlx/math/div_ceil.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/prefix_sum.hpp>
#include <distwt/mpi/uint64_pack.hpp>

//#define DBG_PARSPLIT 1

// performs a parallel split of the given data according to the predicate
//
// num0 and num1 are the amount of data items on which the predicate returns 0
// or 1, respectively, and are expected to be pre-computed beforehand
//
// target_min and target_max describe the (inclusive) range of workers involved
// in the split. the amount of data items received by each worker is balanced
// according to num0 and num1
//
// returns the worker rank at which the data was split
template<typename T, typename predicate_f>
size_t parallel_str_split(
    MPIContext& ctx,
    std::vector<T>& data, // r/w buffer
    predicate_f predicate,
    const size_t local_num0,
    const size_t local_num1,
    const size_t target_min,
    const size_t target_max,
    const int tag) {

    // must have at least 2 targets
    // FIXME: really?
    assert(target_min < target_max);
    const size_t targets = target_max - target_min + 1;

    // local worker must be within the target range
    assert(ctx.rank() >= target_min && ctx.rank() <= target_max);

    // data must be consistent
    assert(local_num0 + local_num1 == data.size());

    // reduce num to global
    std::vector<size_t> num({local_num0, local_num1});
    ctx.all_reduce(num);

    // split items in a balanced manner
    const size_t num_total = num[0] + num[1];

    // FIXME: Computing a 0/1 ratio may result in only one target being
    // available for further computation on either side. This MUST be avoided
    // in case we aren't on the final level of the WT yet. Some sort of a
    // minimum (other than 0) would be required.
    /*
    const double p0 = (double)num[0] / (double)num_total;
    const size_t ceil0 = std::ceil(p0 * (double)targets);
    const size_t targets0 = (num[1] > 0) ? std::min(ceil0, targets-1) : ceil0;
    const size_t targets1 = targets - targets0;

    if(num[0] > 0) assert(targets0 > 0);
    if(num[1] > 0) assert(targets1 > 0);

    std::array<size_t, 2> num_per_target = {
        targets0 == 0 ? 0 : tlx::div_ceil(num[0], targets0),
        targets1 == 0 ? 0 : tlx::div_ceil(num[1], targets1)
    };
    */

    size_t targets0, targets1;
    if(num[0] > 0 && num[1] > 0) {
        targets0 = targets / 2;
        targets1 = targets - targets0;
    } else if(num[0] == 0) {
        targets0 = 0;
        targets1 = targets;
    } else { //if(num[1] == 0) {
        targets0 = targets;
        targets1 = 0;
    }

    std::array<size_t, 2> num_per_target = {
        targets0 == 0 ? 0 : tlx::div_ceil(num[0], targets0),
        targets1 == 0 ? 0 : tlx::div_ceil(num[1], targets1)
    };

    #ifdef DBG_PARSPLIT
    ctx.cout()
        << "local_num0=" << local_num0
        << ", local_num1=" << local_num1 << std::endl;
    ctx.cout_master()
        << "num[0]=" << num[0]
        << ", num[1]=" << num[1] << std::endl;
        //<< ", p0=" << p0 << std::endl;
    ctx.cout_master()
        << "target_min=" << target_min
        << ", target_max=" << target_max << std::endl;
    ctx.cout_master()
        << "targets0=" << targets0
        << ", targets1=" << targets1 << std::endl;
    ctx.cout_master()
        << "num_per_target[0]=" << num_per_target[0]
        << ", num_per_target[1]=" << num_per_target[1] << std::endl;
    #endif

    // compute local 0/1 offsets (prefix sums)
    std::vector<size_t> offs({local_num0, local_num1});
    prefix_sum(ctx, offs);

    #ifdef DBG_PARSPLIT
    ctx.cout() << "offs[0]=" << offs[0]
        << ", offs[1]=" << offs[1] << std::endl;
    ctx.synchronize(); // TODO DEBUG
    #endif

    // allocate message buffer
    using str8_pack_t = uint64_pack_t<unsigned char[8], 8>;

    // TODO: tighter bound than #targets?
    uint64_t** msg_buf = new uint64_t*[targets];
    size_t msg_num = 0;

    // send phase
    {
        // initialize buffers
        std::array<std::vector<T>, 2> buf = {
            std::vector<T>(num_per_target[0]),
            std::vector<T>(num_per_target[1])
        };
        std::array<size_t, 2> glob = {offs[0], offs[1]};
        std::array<size_t, 2> target = {
            num_per_target[0] == 0 ? SIZE_MAX : target_min + glob[0] / num_per_target[0],
            num_per_target[1] == 0 ? SIZE_MAX : target_min + targets0 + glob[1] / num_per_target[1]
        };
        std::array<size_t, 2> p = {glob[0], glob[1]};
        std::array<size_t, 2> count = {0, 0};

        // split and send data
        auto send_interval = [&](const bool b, const size_t to){
            const size_t size = str8_pack_t::required_bufsize(count[b])+2;

            uint64_t* msg = new uint64_t[size];
            msg[0] = glob[b];
            msg[1] = count[b];
            str8_pack_t::pack(buf[b], 0, msg+2, count[b]);
            msg_buf[msg_num++] = msg;

            ctx.isend(msg, size, to, tag);
        };

        for(const T& item : data) {
            const bool b = predicate(item);

            buf[b][count[b]++] = item;
            ++p[b];

            if((p[b] % num_per_target[b]) == 0 && count[b] > 0) {
                // we reached a boundary and should send interval
                // [glob[b], p[b]) to target[b]

                // send
                #ifdef DBG_PARSPLIT
                ctx.cout() << "send [" << glob[b] << "," << glob[b]+count[b]
                           << ") to " << target[b]
                           << " (b=" << int(b) << ")" << std::endl;
                #endif

                assert(p[b] - glob[b] == count[b]);
                send_interval(b, target[b]++);

                // advance
                glob[b] += count[b];
                assert(glob[b] == p[b]);
                count[b] = 0;
            }
        }

        // send remaining items
        for(size_t b = 0; b < 2; b++) {
            if(count[b] > 0) {
                #ifdef DBG_PARSPLIT
                ctx.cout() << "send* [" << glob[b] << "," << glob[b]+count[b]
                           << ") to " << target[b]
                           << " (b=" << b << ")" << std::endl;
                #endif

                send_interval(b, target[b]);
            }
        }
    }

    // receive phase
    {
        // get global role (0/1 and offset)
        const bool b = (ctx.rank() >= target_min + targets0);
        const size_t global_offset = b
            ? (ctx.rank() - target_min - targets0) * num_per_target[1]
            : (ctx.rank() - target_min) * num_per_target[0];

        size_t expect;
        {
            std::array<size_t, 2> last = {target_min+targets0-1, target_max};
            if(ctx.rank() < last[b]) {
                expect = num_per_target[b];
            } else {
                if(num_per_target[b] > 0) {
                    const size_t mod = num[b] % num_per_target[b];
                    expect = (mod == 0) ? num_per_target[b] : mod;
                } else {
                    expect = 0;
                }
            }
        }

        // fit space
        data.resize(expect);
        data.shrink_to_fit();

        #ifdef DBG_PARSPLIT
        ctx.cout() << "b=" << int(b) << ", global_offset=" << global_offset
            << ", expect=" << expect << std::endl;
        #endif

        size_t num_received = 0;
        while(num_received < expect) {
            // probe for message (blocking)
            auto result = ctx.template probe<uint64_t>(tag);

            // receive message
            #ifdef DBG_PARSPLIT
                ctx.cout() << "recv msg of size " << result.size << " from "
                    << result.sender << " ..." << std::endl;
            #endif

            uint64_t* msg = new uint64_t[result.size];
            ctx.recv(msg, result.size, result.sender, tag);

            const size_t moffs = msg[0];
            const size_t mnum = msg[1];

            assert(result.size == str8_pack_t::required_bufsize(mnum) + 2);

            // receive global interval [moffs, moffs+mnum)
            #ifdef DBG_PARSPLIT
            ctx.cout() << "receive ["
                << moffs << ","
                << moffs + mnum
                << ") (" << mnum << " items) from "
                << result.sender << std::endl;
            #endif

            assert(moffs >= global_offset);
            assert(moffs - global_offset + mnum <= data.size());

            str8_pack_t::unpack(msg+2, data, moffs - global_offset, mnum);
            num_received += mnum;
        }
        assert(num_received == expect);
    }

    // synchronize (don't clean buffers before everything has been received)
    ctx.synchronize();

    // clean up
    for(size_t i = 0; i < msg_num; i++) {
        delete[] msg_buf[i];
    }
    delete[] msg_buf;

    // return splitter
    return target_min + targets0;
}
