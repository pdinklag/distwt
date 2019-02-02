#include <distwt/common/effective_alphabet.hpp>

using esym_pack_word_t = uint64_t;
constexpr size_t esym_pack_size = sizeof(esym_pack_word_t) / sizeof(esym_t);

union esym_pack_t {
    esym_t           sym[esym_pack_size];
    esym_pack_word_t packed;
};
