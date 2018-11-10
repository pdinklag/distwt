#include <distwt/thrill/effective_alphabet.hpp>
#include <thrill/api/collapse.hpp>

EffectiveAlphabet::text_t EffectiveAlphabet::transform(
    const rawtext_t& rawtext) const {

    return rawtext
        .Map([this](rawsym_t x){
            return m_map.at(x);
        })
        .Collapse();
}
