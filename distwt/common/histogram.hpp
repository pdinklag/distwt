#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <vector>
#include <numeric>

#include <distwt/common/binary_io.hpp>

template<typename sym_t, typename idx_t>
class HistogramBase {
public:
    using entry_t = std::pair<sym_t, idx_t>;

protected:
    std::vector<entry_t> m_entries;

    void load(const std::string& filename) {
        binary::FileReader r(filename);

        const size_t num_entries = r.template read<size_t>();

        m_entries.clear();
        m_entries.reserve(num_entries);
        for(size_t i = 0; i < num_entries; i++) {
            const auto sym = r.template read<sym_t>();
            const auto cnt = r.template read<idx_t>();

            m_entries.emplace_back(sym, cnt);
        }
    }

public:
    inline HistogramBase() {
    }

    inline HistogramBase(HistogramBase<sym_t, idx_t>&& other) : m_entries(other.m_entries) {
    }

    inline HistogramBase(const HistogramBase<sym_t, idx_t>& other) : m_entries(other.m_entries) {
    }

    inline ~HistogramBase() {
    }

    inline HistogramBase& operator=(HistogramBase<sym_t, idx_t>&& other) {
        m_entries = std::move(other.m_entries);
        return *this;
    }

    inline HistogramBase& operator=(const HistogramBase<sym_t, idx_t>& other) {
        m_entries = other.m_entries;
        return *this;
    }

    // compute C array
    std::vector<idx_t> compute_C() const {
        const size_t num_entries = m_entries.size();

        std::vector<idx_t> c(num_entries + 1);
        c[0] = 0;
        for(size_t i = 1; i <= num_entries; i++) {
            c[i] = c[i-1] + m_entries[i-1].second;
        }
        return c;
    }

    // computes the text length from the histogram
    // (this equals the final entry of the C array)
    size_t text_length() const {
        return std::accumulate(
            m_entries.begin(),
            m_entries.end(),
            size_t(0),
            [](size_t n, const auto& e){ return n + e.second; }
        );
    }

    // save histogram to file
    void save(const std::string& filename) const {
        binary::FileWriter w(filename);

        w.write<size_t>(m_entries.size()); // num entries
        for(auto& e : m_entries) {
            w.write<sym_t>(e.first); // symbol
            w.write<size_t>(e.second);  // count
        }
    }

    // amount of histogram entries
    inline size_t size() const {
        return m_entries.size();
    }

    // direct access to histogram entries
    const std::vector<entry_t>& entries = m_entries;
};

// stream output for Histogram::entry_t
template<typename sym_t, typename idx_t>
inline std::ostream& operator << (
    std::ostream& os, const typename HistogramBase<sym_t, idx_t>::entry_t& p) {

    return os << p.first << " -> " << p.second;
}
