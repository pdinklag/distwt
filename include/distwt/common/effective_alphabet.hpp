#pragma once

#include <unordered_map>

#include <distwt/common/histogram.hpp>

class EffectiveAlphabetBase {
public:
    // a symbol in the effective alphabet
    using symbol_t = unsigned char;

protected:
    std::unordered_map<HistogramBase::symbol_t, symbol_t> m_map;

public:
    EffectiveAlphabetBase(const HistogramBase& hist);
    ~EffectiveAlphabetBase();
};

using esym_t  = EffectiveAlphabetBase::symbol_t;
