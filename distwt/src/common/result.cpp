#include <distwt/common/result.hpp>

#include <sstream>
#include <tlx/string/format_si_iec_units.hpp>

std::string Result::sqlplot() const {
    std::ostringstream oss;
    oss << "RESULT";
    oss << " algo=" << m_algo;
    oss << " nodes=" << m_nodes;
    oss << " workers_per_node=" << m_workers_per_node;
    oss << " input=" << m_input;
    oss << " size=" << m_size;
    oss << " time=" << m_time;
    oss << " traffic=" << m_traffic;
    return oss.str();
}

std::string Result::readable() const {
    std::ostringstream oss;
    oss << "Algorithm '" << m_algo << "' finished processing input '"
        << m_input << "' (" << tlx::format_iec_units(m_size, 3) << "B) after "
        << m_time << " seconds using " << m_nodes << " nodes ("
        << m_workers_per_node << " workers each) causing "
        << tlx::format_iec_units(m_traffic, 3) << "B of net traffic.";
    return oss.str();
}
