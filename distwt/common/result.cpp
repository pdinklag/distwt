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
    oss << " alphabet=" << m_alphabet;
    oss << " time_input=" << m_time.input;
    oss << " time_hist=" << m_time.hist;
    oss << " time_eff=" << m_time.eff;
    oss << " time_construct=" << m_time.construct;
    oss << " time_merge=" << m_time.merge;
    oss << " memory=" << m_memory;
    oss << " traffic=" << m_traffic;
    oss << " traffic_asym=" << m_traffic_asym;
    return oss.str();
}

std::string ResultBase::readable() const {
    std::ostringstream oss;
    oss << "Algorithm '" << m_algo << "' finished processing input '"
        << m_input << "' (" << tlx::format_iec_units(m_size, 3) << "B) after "
        << m_time.total() << " seconds using " << m_nodes << " nodes ("
        << m_workers_per_node << " workers each) causing "
        << tlx::format_iec_units(m_traffic, 3) << "B of net traffic"
        << " (" << tlx::format_iec_units(m_traffic_asym, 3) << "B assymetry) "
        << "using at most " << tlx::format_iec_units(m_memory, 3)
        << "B of overall RAM (avg "
        << tlx::format_iec_units(m_memory / (m_workers_per_node * m_nodes), 3)
        << " per worker).";
    return oss.str();
}
