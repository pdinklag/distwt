#pragma once

#include "def.hpp"

#include <thrill/api/all_gather.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sort.hpp>

// histogram entry
// symbol and number of occurences
using hist_entry_t = std::pair<rawsym_t, size_t>;

// stream output for hist_entry_t
inline std::ostream& operator << (std::ostream& os, const hist_entry_t& p) {
    return os << p.first << " -> " << p.second;
}

// histogram
// vector of lexicographically ordered entries
using hist_t = std::vector<hist_entry_t>;

// compute the histogram of the given input text
hist_t compute_histogram(const rawtext_t& rawtext);
