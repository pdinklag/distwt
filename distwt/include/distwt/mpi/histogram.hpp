#pragma once

#include <distwt/common/histogram.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

class Histogram : public HistogramBase {
public:
    using HistogramBase::HistogramBase;

    Histogram(
        const MPIContext& ctx,
        const FilePartitionReader& input,
        const size_t rdbufsize);

    inline Histogram(const std::string& filename) { // load from file
        load(filename);
    }
};
