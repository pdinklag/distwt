#pragma once

#include <unordered_map>

#include <distwt/common/histogram.hpp>

template<typename sym_t>
class EffectiveAlphabetBase {
protected:
    std::unordered_map<sym_t, sym_t> m_map;

public:
    inline EffectiveAlphabetBase(const HistogramBase<sym_t>& hist) {
        size_t i = 0;
        for(auto e : hist.entries) {
            m_map.emplace(e.first, sym_t(i++));
        }
    }

    inline ~EffectiveAlphabetBase() {
    }
};
