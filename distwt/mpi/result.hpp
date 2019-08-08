#pragma once

#include <distwt/common/result.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <tlx/math/abs_diff.hpp>

class Result : public ResultBase {
public:
    template<typename sym_t>
    inline Result(
        const std::string& algo,
        const MPIContext& ctx,
        const FilePartitionReader<sym_t>& input,
        const size_t alphabet,
        Time time) {

        m_algo = algo;
        m_nodes = ctx.num_nodes();
        m_workers_per_node = ctx.num_workers_per_node();
        m_input = input.filename();
        m_size = input.total_size();
        m_bytes_per_symbol = sizeof(sym_t);
        m_alphabet = alphabet;
        m_time = time;

        // gather distributed stats
        m_memory = ctx.gather_max_alloc();

        auto traffic = ctx.gather_traffic();
        m_traffic = traffic.tx + traffic.tx_est;
        m_traffic_asym = tlx::abs_diff(
            traffic.tx + traffic.tx_est,
            traffic.rx + traffic.rx_est);
    }
};
