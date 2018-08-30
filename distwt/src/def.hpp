#pragma once

#include <thrill/api/dia.hpp>

// a raw input symbol
using rawsym_t = unsigned char;

// a raw distributed text
using rawtext_t = thrill::DIA<rawsym_t>;
