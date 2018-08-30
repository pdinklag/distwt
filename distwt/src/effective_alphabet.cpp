#include "effective_alphabet.hpp"

#include <thrill/api/collapse.hpp>

ea_map_t compute_ea_map(const hist_t& hist) {
    ea_map_t map;

    size_t i = 0;
    for(auto e : hist) {
        map.emplace(e.first, esym_t(i++));
    }

    return map;
}

etext_t compute_effective_transformation(
    const rawtext_t& rawtext, const ea_map_t& eamap) {

    return rawtext
        .Map([eamap](rawsym_t x){
            return eamap.at(x);
        })
        .Collapse()
        .Execute(); // do it NOW
}
