#pragma once

#include <sstream>
#include <tlx/string/format_si_iec_units.hpp>

class Result {
private:
    std::string m_algo;
    size_t m_num_workers;
    std::string m_input;
    size_t m_size;
    double m_time;
    size_t m_traffic;

public:
    inline Result(
        const std::string& algo,
        const size_t num_workers,
        const std::string& input,
        const size_t size,
        const double time,
        const size_t traffic)
        : m_algo(algo),
          m_num_workers(num_workers),
          m_input(input),
          m_size(size),
          m_time(time),
          m_traffic(traffic) {
    }

    inline std::string sqlplot() const {
        std::ostringstream oss;
        oss << "RESULT";
        oss << " algo=" << m_algo;
        oss << " workers=" << m_num_workers;
        oss << " input=" << m_input;
        oss << " size=" << m_size;
        oss << " time=" << m_time;
        oss << " traffic=" << m_traffic;
        return oss.str();
    }

    inline std::string readable() const {
        std::ostringstream oss;
        oss << "Algorithm '" << m_algo << "' finished processing input '"
            << m_input << "' (" << tlx::format_iec_units(m_size, 3) << "B) after "
            << m_time << " seconds using " << m_num_workers << " workers causing "
            << tlx::format_iec_units(m_traffic, 3) << "B of traffic.";
        return oss.str();
    }
};
