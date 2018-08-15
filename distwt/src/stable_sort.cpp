#include <algorithm>
#include <iostream>
#include <string>

#include <thrill/api/read_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/write_lines.hpp>

using Pair = std::pair<size_t, size_t>;

std::ostream& operator << (std::ostream& os, const Pair& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}

class SortAlgorithm {
public:
    template<typename Iterator, typename CompareFunction>
    void operator () (Iterator begin, Iterator end, CompareFunction cmp) const {
        return std::sort(begin, end, cmp);
    }
};

void StableSort(
    thrill::Context& ctx, std::string input) {

    using Pair = std::pair<size_t, size_t>;

    thrill::ReadLines(ctx, input)
        .Map([](const std::string& line){
                const size_t x = line.find(' ');

                const size_t key = std::stoi(line.substr(0,x));
                const size_t value = std::stoi(line.substr(x+1));
                return Pair(key, value);
            })
        .Sort([](const Pair& a, const Pair& b){
                return a.first < b.first;
            }, SortAlgorithm())
        .Print("sorted");
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <input>" << std::endl;
        return -1;
    }

    return thrill::Run([&](thrill::Context& ctx){
        StableSort(ctx, argv[1]);
    });
}

