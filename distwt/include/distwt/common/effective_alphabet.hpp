#pragma once

#include <unordered_map>

#include <distwt/common/histogram.hpp>

class EffectiveAlphabetBase {
public:
    // an entry index into the effective alphabet
    using index_t = unsigned char;

    // a symbol within an effective alphabet
    struct symbol_t {
    private:
        index_t m_idx;

    public:
        inline symbol_t(){}

        inline symbol_t(index_t idx)       : m_idx(idx){}
        inline symbol_t(const symbol_t& x) : m_idx(x.m_idx){}
        inline symbol_t(symbol_t&& x)      : m_idx(x.m_idx){}

        inline operator index_t() const { return m_idx; }

        inline symbol_t& operator =(const symbol_t& x) {
            m_idx = x.m_idx;
            return *this;
        }

        inline symbol_t& operator =(index_t idx) {
            m_idx = idx;
            return *this;
        }

        inline bool operator <(const symbol_t& x) const {
            return m_idx < x.m_idx;
        }

        const index_t& index = m_idx;
    };

protected:
    std::unordered_map<HistogramBase::symbol_t, symbol_t> m_map;

public:
    EffectiveAlphabetBase(const HistogramBase& hist);
    ~EffectiveAlphabetBase();
};

using esym_t  = EffectiveAlphabetBase::symbol_t;

// stream output for esym_t
inline std::ostream& operator<<(
    std::ostream& os, const esym_t& x) {

    os << size_t(x.index);
    return os;
}
