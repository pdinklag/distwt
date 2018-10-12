#include <thrill/api/all_gather.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sort.hpp>

#include <distwt/histogram.hpp>
#include <distwt/binary_io.hpp>

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

Histogram::Histogram(const std::string& filename) {
    binary::FileReader r(filename);

    const size_t num_entries = r.template read<size_t>();

    m_entries.reserve(num_entries);
    for(size_t i = 0; i < num_entries; i++) {
        const auto sym = r.template read<rawsym_t>();
        const auto cnt = r.template read<size_t>();

        m_entries.emplace_back(sym, cnt);
    }
}

void Histogram::save(const std::string& filename) const {
    binary::FileWriter w(filename);

    w.write<size_t>(m_entries.size()); // num entries
    for(auto& e : m_entries) {
        w.write<rawsym_t>(e.first); // symbol
        w.write<size_t>(e.second);  // count
    }
}


