#include <numeric>

#include <thrill/api/all_gather.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sort.hpp>

#include <distwt/thrill/histogram.hpp>

Histogram::Histogram(const rawtext_t& rawtext) {
    m_entries = rawtext
        .Map([](rawsym_t x){
            // map each symbol to an occurence counter
            return entry_t(x, 1);
        })
        .ReduceByKey(
            [](const entry_t& e){
                // use symbol value as extraction key
                return e.first;
            },
            [](const entry_t& a, const entry_t& b){
                // reduce by summing up occurences
                return entry_t(a.first, a.second+b.second);
            }
        )
        .Sort([](const entry_t& a, const entry_t& b){
            // sort lexicographically
            return a.first < b.first;
        })
        .Execute()
        .AllGather();
}
