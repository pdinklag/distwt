#pragma once

#include <distwt/mpi/wt.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

class WaveletTreeLevelwise; // fwd
class WaveletTreeNodebased : public WaveletTree {
public:
    inline WaveletTreeNodebased(
        const HistogramBase& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    // void save(const std::string& filename);

    WaveletTreeLevelwise merge(
        MPIContext& ctx,
        const FilePartitionReader& input,
        const HistogramBase& hist,
        bool discard);
};
