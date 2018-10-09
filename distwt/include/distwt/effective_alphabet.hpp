#pragma once

#include <unordered_map>

#include <distwt/text.hpp>
#include <distwt/histogram.hpp>

// type used for effective alphabet indices
using ea_index_t = unsigned char;

// an effective alphabet entry, ie, an index within an effective alphabet
struct esym_t {
private:
    ea_index_t m_idx;

public:
    inline esym_t() {
    }

    inline esym_t(ea_index_t idx) : m_idx(idx) {
    }

    inline esym_t(const esym_t& x) : m_idx(x.m_idx) {
    }

    inline esym_t(esym_t&& x) : m_idx(x.m_idx) {
    }

    inline operator ea_index_t() const {
        return m_idx;
    }

    esym_t& operator =(const esym_t& x) {
        m_idx = x.m_idx;
        return *this;
    }

    esym_t& operator =(ea_index_t idx) {
        m_idx = idx;
        return *this;
    }

    bool operator <(const esym_t& x) const {
        return m_idx < x.m_idx;
    }

    const ea_index_t& index = m_idx;
};

// stream output for esym_t
inline std::ostream& operator<<(std::ostream& os, const esym_t& x) {
    os << size_t(x.index);
    return os;
}

// serialization for esym_t
template<typename Archive>
struct thrill::data::Serialization<Archive, esym_t>{
    static void Serialize(const esym_t& x, Archive& ar) {
        ar.PutRaw(x.index);
    }
    static esym_t Deserialize(Archive& ar) {
        return esym_t(ar.template GetRaw<ea_index_t>());
    }
    static constexpr bool   is_fixed_size = true;
    static constexpr size_t fixed_size    = sizeof(ea_index_t);
};

// effective alphabet (ea) mapping
// maps a symbol to its effective counterpart, ie, index in the ea
using ea_map_t = std::unordered_map<rawsym_t, esym_t>;

// effective transformation of a text
using etext_t = thrill::DIA<esym_t>;

// compute EA map given a histogram
ea_map_t compute_ea_map(const Histogram& hist);

// compute effective transformation of the given text
etext_t compute_effective_transformation(
    const rawtext_t& rawtext, const ea_map_t& eamap);
