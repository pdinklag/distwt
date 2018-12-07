#pragma once

#include <distwt/mpi/wt.hpp>
#include <distwt/mpi/context.hpp>

class WaveletTreeLevelwise : public WaveletTree {
public:
    inline WaveletTreeLevelwise(
        const HistogramBase& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    void save(const MPIContext& ctx, const std::string& output);
};
