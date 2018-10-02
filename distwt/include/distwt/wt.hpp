#pragma once

#include <vector>
#include <thrill/api/dia.hpp>

// DIA type used for distributed bit vectors
using bv_dia_t = thrill::DIA<bool>;

// storage for a wavelet tree's bit vector
using wt_bits_t = std::vector<bv_dia_t>;
