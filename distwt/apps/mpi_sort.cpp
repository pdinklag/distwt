#include <tlx/cmdline_parser.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/stable_sort.hpp>

#ifndef DBG_SORT
template<typename T>
void print_vector(MPIContext& ctx, std::vector<T>& v, std::string name) {
    auto& out = ctx.cout();
    out << name << ": [ ";
    for(const T& x : v) out << x << " ";
    out << "]" << std::endl;
}
#endif

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    std::vector<uint32_t> v;
    switch(ctx.rank()) {
        case 0: v = { 7, 5, 2, 1, 4, 1, 8, 9, 4, 9, 4, 2, 1, 3 }; break;
        case 1: v = { 4, 1, 3, 8, 4, 2, 1, 3, 6, 7, 7, 3, 4, 5 }; break;
        case 2: v = { 0, 6, 7, 9, 9, 1, 0, 5, 4, 1, 0, 2, 5, 4 }; break;
        case 3: v = { 3, 5, 2, 6, 0, 8, 3, 2, 7, 6, 8, 7, 5, 3 }; break;
        case 4: v = { 2, 2, 2, 1, 7, 8, 9, 3, 0, 4, 4, 6, 1, 3 }; break;
        case 5: v = { 6, 7, 4, 9, 0, 1, 4, 3, 2, 6, 8, 9, 3, 1 }; break;
        case 6: v = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3 }; break;
        case 7: v = { 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 8 }; break;
    }

    print_vector(ctx, v, "input");

    ctx.synchronize();

    struct KeyCompare {
        inline bool operator()(sortkey_t a, sortkey_t b) const {
            return a < b;
        }
    };
    stable_sort(
        ctx,
        v,
        [](uint32_t x) -> sortkey_t{return x & 0x3;},
        KeyCompare(),
        ctx.num_workers());

    ctx.synchronize();
    print_vector(ctx, v, "output");

    return 0;
}
