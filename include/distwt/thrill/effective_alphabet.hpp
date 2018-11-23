#pragma once

#include <distwt/common/effective_alphabet.hpp>
#include <distwt/thrill/text.hpp>

class EffectiveAlphabet : public EffectiveAlphabetBase {
public:
    using EffectiveAlphabetBase::EffectiveAlphabetBase;

    // effective transformation of a text
    using text_t = thrill::DIA<symbol_t>;

    text_t transform(const rawtext_t& rawtext) const;
};

// convenience aliases
using etext_t = EffectiveAlphabet::text_t;
