#include <distwt/thrill/result.hpp>

#include <foxxll/io/iostats.hpp>
#include <tlx/math/abs_diff.hpp>

// taken from Thrill's OverallStats (context.cpp) and reduced to what I need
struct ThrillStats {
    size_t max_block_bytes;
    size_t net_traffic_tx, net_traffic_rx;
    size_t io_volume;
    size_t io_max_allocation;

    ThrillStats operator + (const ThrillStats& b) const {
        ThrillStats r;
        r.max_block_bytes = max_block_bytes + b.max_block_bytes;
        r.net_traffic_tx = net_traffic_tx + b.net_traffic_tx;
        r.net_traffic_rx = net_traffic_rx + b.net_traffic_rx;
        r.io_volume = io_volume + b.io_volume;
        r.io_max_allocation = std::max(io_max_allocation, b.io_max_allocation);
        return r;
    }
};

Result::Result(
    const std::string& algo,
    thrill::Context& ctx,
    const std::string& input_filename,
    const size_t input_size,
    const size_t alphabet,
    const double time_input,
    const double time_construct) {

    m_algo = algo;
    m_nodes = ctx.num_hosts();
    m_workers_per_node = ctx.workers_per_host();
    m_input = input_filename;
    m_size = input_size;
    m_alphabet = alphabet;
    m_time_input = time_input;
    m_time_construct = time_construct;

    // compute stats exactly like Thrill does it
    const size_t local_worker_id_ = ctx.local_worker_id();
    const size_t local_host_id_ = ctx.local_host_id();
    const auto& net_manager_ = ctx.net_manager();

    ThrillStats stats;
    stats.max_block_bytes =
        local_worker_id_ == 0 ? ctx.block_pool().max_total_bytes() : 0;

    stats.net_traffic_tx = local_worker_id_ == 0 ? net_manager_.Traffic().tx : 0;
    stats.net_traffic_rx = local_worker_id_ == 0 ? net_manager_.Traffic().rx : 0;

    if (local_host_id_ == 0 && local_worker_id_ == 0) {
        foxxll::stats_data io_stats(*foxxll::stats::get_instance());
        stats.io_volume = io_stats.get_read_bytes() + io_stats.get_write_bytes();
        stats.io_max_allocation =
            foxxll::block_manager::get_instance()->maximum_allocation();
    } else {
        stats.io_volume = 0;
        stats.io_max_allocation = 0;
    }

    stats = ctx.net.Reduce(stats);

    // read from Thrill stats
    m_memory = stats.max_block_bytes;
    m_traffic = stats.net_traffic_tx;
    m_traffic_asym = tlx::abs_diff(stats.net_traffic_tx, stats.net_traffic_rx);
}
