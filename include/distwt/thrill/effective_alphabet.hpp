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

