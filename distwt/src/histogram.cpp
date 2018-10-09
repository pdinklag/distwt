#include <thrill/api/all_gather.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sort.hpp>

#include <distwt/histogram.hpp>
#include <distwt/binary_io.hpp>

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

void save_histogram(const hist_t& hist, const std::string& filename) {
    binary::FileWriter w(filename);

    w.write<size_t>(hist.size()); // num entries
    for(auto& e : hist) {
        w.write<rawsym_t>(e.first); // symbol
        w.write<size_t>(e.second);  // count
    }
}

hist_t load_histogram(const std::string& filename) {
    binary::FileReader r(filename);

    const size_t num_entries = r.template read<size_t>();
    hist_t hist(num_entries);

    for(size_t i = 0; i < num_entries; i++) {
        const auto sym = r.template read<rawsym_t>();
        const auto cnt = r.template read<size_t>();

        hist[i] = hist_entry_t(sym, cnt);
    }
    return hist;
}
