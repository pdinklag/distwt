#pragma once

#include <string>

class ResultBase {
protected:
    std::string m_algo;
    size_t m_nodes;
    size_t m_workers_per_node;
    std::string m_input;
    size_t m_size;
    size_t m_alphabet;
    double m_time_input;
    double m_time_construct;
    size_t m_memory;
    size_t m_traffic;
    size_t m_traffic_asym;

public:
    std::string sqlplot() const;
    std::string readable() const;
};
