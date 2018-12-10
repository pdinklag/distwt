#pragma once

#include <distwt/common/result.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

class Result : public ResultBase {
public:
    Result(
        const std::string& algo,
        const MPIContext& ctx,
        const FilePartitionReader& input,
        const size_t alphabet,
        const double time_input,
        const double time_construct);
};
