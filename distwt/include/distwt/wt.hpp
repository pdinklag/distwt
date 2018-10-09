#pragma once

// DIA type used for distributed bit vectors
using bv_dia_t = thrill::DIA<uint64_t>;

// storage for a wavelet tree's bit vector
using wt_bits_t = std::vector<bv_dia_t>;
