#pragma once

#include <string>
#include <tuple>

#include <distwt/text.hpp>

class Histogram {
public:
    using entry_t = std::pair<rawsym_t, size_t>;

private:
    std::vector<entry_t> m_entries;

public:
    inline Histogram() {
    }

    inline Histogram(Histogram&& other) : m_entries(other.m_entries) {
    }

    inline Histogram(const Histogram& other) : m_entries(other.m_entries) {
    }

    Histogram(const rawtext_t& rawtext); // compute from text
    Histogram(const std::string& filename); // load from file

    inline ~Histogram() {
    }

    inline Histogram& operator=(Histogram&& other) {
        m_entries = std::move(other.m_entries);
        return *this;
    }

    inline Histogram& operator=(const Histogram& other) {
        m_entries = other.m_entries;
        return *this;
    }

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
    std::ostream& os, const Histogram::entry_t& p) {

    return os << p.first << " -> " << p.second;
}
