#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include <distwt/common/symbol.hpp>

class HistogramBase {
public:
    using entry_t = std::pair<symbol_t, size_t>;

protected:
    std::vector<entry_t> m_entries;

    void load(const std::string& filename);

public:
    inline HistogramBase() {
    }

    inline HistogramBase(HistogramBase&& other) : m_entries(other.m_entries) {
    }

    inline HistogramBase(const HistogramBase& other) : m_entries(other.m_entries) {
    }

    inline ~HistogramBase() {
    }

    inline HistogramBase& operator=(HistogramBase&& other) {
        m_entries = std::move(other.m_entries);
        return *this;
    }

    inline HistogramBase& operator=(const HistogramBase& other) {
        m_entries = other.m_entries;
        return *this;
    }

    // compute C array
    std::vector<size_t> compute_C() const;

    // computes the text length from the histogram
    // (this equals the final entry of the C array)
    size_t text_length() const;

    // save histogram to file
    void save(const std::string& filename) const;

    // amount of histogram entries
    inline size_t size() const {
        return m_entries.size();
    }

    // direct access to histogram entries
    const std::vector<entry_t>& entries = m_entries;
};

// stream output for Histogram::entry_t
inline std::ostream& operator << (
    std::ostream& os, const HistogramBase::entry_t& p) {

    return os << p.first << " -> " << p.second;
}
