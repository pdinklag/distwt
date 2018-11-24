#pragma once

#include <algorithm>
#include <vector>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/mpi_type.hpp>

// computes the prefix sum of vector v
// after the operation, on the j-th worker, v contains the sum of the vectors
// of nodes [0,j] (if inclusive is true) or [0,j) (if inclusive is false)
template<typename T>
void prefix_sum(
    MPIContext& ctx,
    std::vector<T>& v,
    bool inclusive = false) {

    const size_t num = v.size();

    // send data to workers with a higher rank
    for(size_t to = ctx.rank() + 1; to < ctx.num_workers(); to++) {
        ctx.send(v, to);
    }

    // if not inclusive, reset all entries to zero
    if(!inclusive) {
        std::fill(v.begin(), v.end(), (T)0);
    }

    // receive data from workers with lower rank
    std::vector<T> buf;
    for(size_t from = 0; from < ctx.rank(); from++) {
        ctx.recv(buf, num, from);

        // compute prefix sum
        for(size_t i = 0; i < num; i++) {
            v[i] += buf[i];
        }
    }
}
