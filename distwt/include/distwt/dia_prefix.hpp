#pragma once

#include <thrill/api/dia.hpp>
#include <thrill/api/window.hpp>

template<typename value_t, typename dia_t>
auto dia_prefix(
    const dia_t& dia,
    const size_t length,
    const size_t window = 512ULL) {

    return dia.template FlatWindow<value_t>(thrill::api::DisjointTag, window,
        [length](size_t index, const std::vector<value_t>& v, auto emit) {
            // cut off items beyond desired prefix length
            for(size_t i = 0; index + i < length && i < v.size(); i++) {
                emit(v[i]);
            }
    });
}
