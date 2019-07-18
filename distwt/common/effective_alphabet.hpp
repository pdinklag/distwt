#pragma once

#include <unordered_map>

#include <distwt/common/histogram.hpp>
#include <distwt/common/symbol.hpp>

class EffectiveAlphabetBase {
public:
    using esym_t = symbol_t;

protected:
    std::unordered_map<symbol_t, esym_t> m_map;

public:
    EffectiveAlphabetBase(const HistogramBase& hist);
    ~EffectiveAlphabetBase();
};

using esym_t = EffectiveAlphabetBase::esym_t;
