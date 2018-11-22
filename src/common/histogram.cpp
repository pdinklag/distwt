#include <numeric>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/histogram.hpp>

void HistogramBase::load(const std::string& filename) {
    binary::FileReader r(filename);

    const size_t num_entries = r.template read<size_t>();

    m_entries.clear();
    m_entries.reserve(num_entries);
    for(size_t i = 0; i < num_entries; i++) {
        const auto sym = r.template read<symbol_t>();
        const auto cnt = r.template read<size_t>();

        m_entries.emplace_back(sym, cnt);
    }
}

std::vector<size_t> HistogramBase::compute_C() const {
    const size_t num_entries = m_entries.size();

    std::vector<size_t> c(num_entries + 1);
    c[0] = 0;
    for(size_t i = 1; i <= num_entries; i++) {
        c[i] = c[i-1] + m_entries[i-1].second;
    }
    return c;
}

size_t HistogramBase::text_length() const {
    return std::accumulate(
        m_entries.begin(),
        m_entries.end(),
        size_t(0),
        [](size_t n, const auto& e){ return n + e.second; }
    );
}

void HistogramBase::save(const std::string& filename) const {
    binary::FileWriter w(filename);

    w.write<size_t>(m_entries.size()); // num entries
    for(auto& e : m_entries) {
        w.write<symbol_t>(e.first); // symbol
        w.write<size_t>(e.second);  // count
    }
}


