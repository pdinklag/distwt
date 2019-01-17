#pragma once

#include <cassert>

#include <distwt/mpi/wt.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>
#include <distwt/mpi/uint64_pack_bv64.hpp>

class WaveletTreeLevelwise; // fwd
class WaveletTreeNodebased : public WaveletTree {
public:
    inline WaveletTreeNodebased(
        const HistogramBase& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    WaveletTreeLevelwise merge(
        MPIContext& ctx,
        const FilePartitionReader& input,
        const HistogramBase& hist,
        bool discard);
};
