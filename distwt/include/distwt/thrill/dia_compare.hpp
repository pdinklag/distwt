#pragma once

#include <thrill/api/dia.hpp>

#include <thrill/api/sum.hpp>
#include <thrill/api/zip.hpp>

template<typename value_t, typename dia_a_t, typename dia_b_t>
size_t dia_compare(const dia_a_t& a, const dia_b_t& b) {
    return a
        .Zip(b, [](const value_t& va, const value_t& vb){
            // test if single items are equal
            return (va != vb) ? 1ULL : 0ULL;
        })
        .Sum([](size_t da, size_t db){
            // sum up amount of differing items (diff)
            return da + db;
        }, 0ULL);
}
