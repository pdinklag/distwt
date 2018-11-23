#include <distwt/common/result.hpp>

#include <sstream>
#include <tlx/string/format_si_iec_units.hpp>

std::string ResultBase::sqlplot() const {
    std::ostringstream oss;
    oss << "RESULT";
    oss << " algo=" << m_algo;
    oss << " nodes=" << m_nodes;
    oss << " workers_per_node=" << m_workers_per_node;
    oss << " input=" << m_input;
    oss << " size=" << m_size;
    oss << " time=" << m_time;
    oss << " memory=" << m_memory;
    oss << " traffic=" << m_traffic;
    oss << " traffic_asym=" << m_traffic_asym;
    return oss.str();
}

std::string ResultBase::readable() const {
    std::ostringstream oss;
    oss << "Algorithm '" << m_algo << "' finished processing input '"
        << m_input << "' (" << tlx::format_iec_units(m_size, 3) << "B) after "
        << m_time << " seconds using " << m_nodes << " nodes ("
        << m_workers_per_node << " workers each) causing "
        << tlx::format_iec_units(m_traffic, 3) << "B of net traffic"
        << " (" << tlx::format_iec_units(m_traffic_asym, 3) << "B assymetry) "
        << "using at most " << tlx::format_iec_units(m_memory, 3)
        << "B of RAM.";
    return oss.str();
}
