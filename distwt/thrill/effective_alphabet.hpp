#pragma once

#include <distwt/common/effective_alphabet.hpp>
#include <distwt/thrill/text.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

class EffectiveAlphabet : public EffectiveAlphabetBase<sym_t> {
public:
    using EffectiveAlphabetBase::EffectiveAlphabetBase;

    // effective transformation of a text
    using text_t = thrill::DIA<sym_t>;

    text_t transform(const rawtext_t& rawtext) const;
};

// convenience aliases
using etext_t = EffectiveAlphabet::text_t;
