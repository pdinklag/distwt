#include <distwt/effective_alphabet.hpp>

#include <thrill/api/collapse.hpp>

EffectiveAlphabet::EffectiveAlphabet(const Histogram& hist) {
    size_t i = 0;
    for(auto e : hist.entries) {
        m_map.emplace(e.first, symbol_t(i++));
    }
}

EffectiveAlphabet::~EffectiveAlphabet() {
}

EffectiveAlphabet::text_t EffectiveAlphabet::transform(
    const rawtext_t& rawtext) const {

    return rawtext
        .Map([this](rawsym_t x){
            return m_map.at(x);
        })
        .Collapse();
}
