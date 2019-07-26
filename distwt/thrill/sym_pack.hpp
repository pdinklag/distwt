#include <distwt/common/effective_alphabet.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

// FIXME: this will break when sym_t becomes wider than 32 bits!

using sym_pack_word_t = uint64_t;
constexpr size_t sym_pack_size = sizeof(sym_pack_word_t) / sizeof(sym_t);

union sym_pack_t {
    sym_t           sym[sym_pack_size];
    sym_pack_word_t packed;
};
