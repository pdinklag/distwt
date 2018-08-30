#pragma once

#include <thrill/api/dia.hpp>

#include <thrill/api/sum.hpp>
#include <thrill/api/zip.hpp>

template<typename ValueType>
size_t dia_compare(
    const thrill::DIA<ValueType>& a,
    const thrill::DIA<ValueType>& b) {

    return a
        .Zip(b, [](const ValueType& a, const ValueType& b){
            // test if single items are equal
            return (a != b) ? 1ULL : 0ULL;
        })
        .Sum([](size_t a, size_t b){
            // sum up amount of differing items (diff)
            return a + b;
        }, 0ULL);
}
