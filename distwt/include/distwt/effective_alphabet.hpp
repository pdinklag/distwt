#pragma once

#include <unordered_map>

#include <distwt/text.hpp>
#include <distwt/histogram.hpp>

class EffectiveAlphabet {
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

    // effective transformation of a text
    using text_t = thrill::DIA<symbol_t>;

private:
    std::unordered_map<rawsym_t, symbol_t> m_map;

public:
    EffectiveAlphabet(const Histogram& hist);
    ~EffectiveAlphabet();

    text_t transform(const rawtext_t& rawtext) const;
};

// convenience aliases
using esym_t  = EffectiveAlphabet::symbol_t;
using etext_t = EffectiveAlphabet::text_t;

// stream output for EffectiveAlphabet::symbol_t
inline std::ostream& operator<<(
    std::ostream& os, const EffectiveAlphabet::symbol_t& x) {

    os << size_t(x.index);
    return os;
}

// serialization for EffectiveAlphabet::symbol_t
template<typename Archive>
struct thrill::data::Serialization<Archive, esym_t>{
    static void Serialize(const esym_t& x, Archive& ar) {
        ar.PutRaw(x.index);
    }
    static esym_t Deserialize(Archive& ar) {
        return esym_t(ar.template GetRaw<EffectiveAlphabet::index_t>());
    }
    static constexpr bool   is_fixed_size = true;
    static constexpr size_t fixed_size    = sizeof(EffectiveAlphabet::index_t);
};
