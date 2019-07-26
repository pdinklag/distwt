#pragma once

#include <vector>
#include <unordered_map>

template<typename K, typename V>
void extract_map(
    const std::unordered_map<K, V>& map,
    std::vector<K>& k,
    std::vector<V>& v) {

    const size_t n = map.size();
    k.resize(n);
    v.resize(n);

    size_t i = 0;
    for(const auto& it : map) {
        k[i] = it.first;
        v[i] = it.second;
        ++i;
    }
}
