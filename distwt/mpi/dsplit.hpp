#pragma once

#include <array>
#include <cmath>
#include <cassert>
#include <functional>
#include <tlx/math/div_ceil.hpp>

#include <distwt/mpi/context.hpp>

//#define DBG_PARSPLIT 1

// performs a distributed split of the given data according to the predicate
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
size_t dsplit_str(
    MPIContext& ctx,
    std::vector<T>& data, // r/w buffer
    predicate_f predicate,
    const size_t local_num0,
    const size_t local_num1,
    const int tag) {

    // must have at least 2 targets
    const size_t targets = ctx.num_workers();
    assert(targets >= 2);

    // data must be consistent
    const size_t local_num_total = local_num0 + local_num1;
    assert(local_num_total == data.size());

    // reduce num to global
    std::vector<size_t> num({local_num0, local_num1});
    ctx.all_reduce(num);

    // split items in a balanced manner
    const size_t num_total = num[0] + num[1];

    // compute amount of workers for 0/1 parts based on ratio
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

    #ifdef DBG_PARSPLIT
    ctx.cout()
        << "local_num0=" << local_num0
        << ", local_num1=" << local_num1 << std::endl;
    ctx.cout_master()
        << "num[0]=" << num[0]
        << ", num[1]=" << num[1] << std::endl;
        << ", p0=" << p0 << std::endl;
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
    ctx.ex_scan(offs);

    #ifdef DBG_PARSPLIT
    ctx.cout() << "offs[0]=" << offs[0]
        << ", offs[1]=" << offs[1] << std::endl;
    ctx.synchronize(); // TODO DEBUG
    #endif

    // allocate message buffer
    std::vector<uint64_t*> msg_headers;
    std::vector<T> msg_buf(local_num_total);

    // send phase
    {
        // initialize buffers
        std::array<T*, 2> buf = {
            msg_buf.data(),
            msg_buf.data() + local_num0
        };
        std::array<size_t, 2> glob = {offs[0], offs[1]};
        std::array<size_t, 2> target = {
            num_per_target[0] == 0 ? SIZE_MAX : glob[0] / num_per_target[0],
            num_per_target[1] == 0 ? SIZE_MAX : targets0 + glob[1] / num_per_target[1]
        };
        std::array<size_t, 2> p = {glob[0], glob[1]};
        std::array<size_t, 2> count = {0, 0};

        // split and send data
        auto send_interval = [&](const bool b, const size_t to){
            uint64_t* header = new uint64_t[2];
            header[0] = glob[b];
            header[1] = count[b];
            msg_headers.push_back(header);

            ctx.isend(header, 2, to, tag);
            ctx.isend(buf[b], count[b], to, tag);
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
                buf[b] += count[b];
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
        const bool b = (ctx.rank() >= targets0);
        const size_t global_offset = b
            ? (ctx.rank() - targets0) * num_per_target[1]
            : (ctx.rank()) * num_per_target[0];

        size_t expect;
        {
            std::array<size_t, 2> last = {targets0-1, ctx.num_workers()-1};
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
        data = std::vector<T>(expect);

        #ifdef DBG_PARSPLIT
        ctx.cout() << "b=" << int(b) << ", global_offset=" << global_offset
            << ", expect=" << expect << std::endl;
        #endif

        uint64_t rheader[2];
        size_t num_received = 0;
        while(num_received < expect) {
            // probe for message (blocking)
            auto result = ctx.template probe<uint64_t>(tag);

            // receive message
            assert(result.size == 2ULL);
            ctx.recv(rheader, 2, result.sender, tag);

            const size_t moffs = rheader[0];
            const size_t mnum = rheader[1];

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

            const size_t local_offs = moffs - global_offset;

            ctx.recv(data.data() + local_offs, mnum, result.sender, tag);
            num_received += mnum;
        }
        assert(num_received == expect);
    }

    // synchronize (don't clean buffers before everything has been received)
    ctx.synchronize();

    // clean up
    for(uint64_t* header : msg_headers) {
        delete[] header;
    }

    // return splitter
    return targets0;
}
