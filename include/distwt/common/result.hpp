#pragma once

#include <string>

class Result {
private:
    std::string m_algo;
    size_t m_nodes;
    size_t m_workers_per_node;
    std::string m_input;
    size_t m_size;
    double m_time;
    size_t m_traffic;

public:
    inline Result(
        const std::string& algo,
        const size_t nodes,
        const size_t workers_per_node,
        const std::string& input,
        const size_t size,
        const double time,
        const size_t traffic)
        : m_algo(algo),
          m_nodes(nodes),
          m_workers_per_node(workers_per_node),
          m_input(input),
          m_size(size),
          m_time(time),
          m_traffic(traffic) {
    }

    std::string sqlplot() const;
    std::string readable() const;
};
