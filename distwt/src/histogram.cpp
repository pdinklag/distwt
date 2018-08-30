#include "histogram.hpp"

#include <thrill/api/all_gather.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sort.hpp>

hist_t compute_histogram(const rawtext_t& rawtext) {
    return rawtext
        .Map([](rawsym_t x){
            // map each symbol to an occurence counter
            return hist_entry_t(x, 1);
        })
        .ReduceByKey(
            [](const hist_entry_t& e){
                // use symbol value as extraction key
                return e.first;
            },
            [](const hist_entry_t& a, const hist_entry_t& b){
                // reduce by summing up occurences
                return hist_entry_t(a.first, a.second+b.second);
            }
        )
        .Sort([](const hist_entry_t& a, const hist_entry_t& b){
            // sort lexicographically
            return a.first < b.first;
        })
        .Execute()
        .AllGather();
}
