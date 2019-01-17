#pragma once

#include <algorithm>
#include <vector>

#include <distwt/mpi/context.hpp>

class PrefixSumOneSuperstep {
public:
    // computes the prefix sum of vector v
    // after the operation, on the j-th worker, v contains the sum
    // of nodes [0,j] (if inclusive is true) or [0,j) (if inclusive is false)
    template<typename T>
    void operator()(
        MPIContext& ctx,
        std::vector<T>& v,
        bool inclusive = false) {

        const size_t num = v.size();

        // TODO a possible optimization to reduce network traffic:
        // - instead of sending data to every worker, only send it to the first
        //   relevant worker of each node
        // - in a second step, that worker will pass on the information to the
        //   remaining workers on the same node, but in shared memory

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
};

class PrefixSumChain {
public:
    // computes the prefix sum of vector v using less messages, but more
    // synchronization
    // after the operation, on the j-th worker, v contains the sum
    // of nodes [0,j] (if inclusive is true) or [0,j) (if inclusive is false)
    template<typename T>
    void operator()(
        MPIContext& ctx,
        std::vector<T>& v,
        bool inclusive = false) {

        const size_t rank = ctx.rank();
        const size_t num = v.size();

        std::vector<T> buf;
        if(rank > 0) {
            // receive from previous worker
            ctx.recv(buf, num, rank - 1);
        } else {
            // initialize
            buf.resize(num, (T)0);
        }

        if(!inclusive) {
            // "swap"
            for(size_t i = 0; i < num; i++) {
                const size_t t = buf[i];
                buf[i] += v[i];
                v[i] = t;
            }
        } else {
            // copy
            for(size_t i = 0; i < num; i++) {
                buf[i] += v[i];
            }
            v = buf;
        }

        if(rank+1 < ctx.num_workers()) {
            // send to next worker
            ctx.send(buf, rank+1);
        }
    }
};

class PrefixSumMPIScan {
public:
    // computes the prefix sum of vector v using MPI's scan operations.
    // after the operation, on the j-th worker, v contains the sum
    // of nodes [0,j] (if inclusive is true) or [0,j) (if inclusive is false)
    template<typename T>
    void operator()(
        MPIContext& ctx,
        std::vector<T>& v,
        bool inclusive = false) {

        if(inclusive) {
            ctx.scan(v);
        } else {
            ctx.ex_scan(v);
        }
    }
};

