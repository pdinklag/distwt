#include <distwt/common/effective_alphabet.hpp>

EffectiveAlphabetBase::EffectiveAlphabetBase(const HistogramBase& hist) {
    size_t i = 0;
    for(auto e : hist.entries) {
        m_map.emplace(e.first, symbol_t(i++));
    }
}

EffectiveAlphabetBase::~EffectiveAlphabetBase() {
}
