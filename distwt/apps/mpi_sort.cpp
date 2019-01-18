#include <algorithm>
#include <random>
#include <utility>
#include <vector>

#include <tlx/cmdline_parser.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/mpi_type.hpp>

template<typename T>
void print_vector(MPIContext& ctx, std::vector<T>& v, std::string name) {
    auto& out = ctx.cout();
    out << name << ": [ ";
    for(uint32_t x : v) out << x << " ";
    out << "]" << std::endl;
}

constexpr size_t MASTER = 0;

// find rank of greatest key <= "it"
// may go one beyond the right bound!
template<typename key_t, typename key_compare_t>
size_t lb_rank(
    const std::vector<key_t>& keys,
    const key_t& it,
    key_compare_t key_compare) {

    size_t a = 0, b = keys.size()-1;
    while(a < b) {
        const size_t m = (a+b)/2;
        if(key_compare(it, keys[m])) {
            // go left
            b = m;
        } else {
            // go right
            a = m+1;
        }
    }

    if(key_compare(it, keys[a])) {
        return a;
    } else {
        return a+1;
    }
}

template<typename T, typename key_extractor_t, typename key_compare_t>
void stable_sort(
    MPIContext& ctx,
    std::vector<T>& v,
    key_extractor_t key,   // T -> key_t
    key_compare_t key_compare, // (key_t, key_t) -> bool
    const size_t a // oversampling factor
) {

    const size_t p = ctx.num_workers();

    // compare elements using their keys
    const auto item_compare = [key,key_compare](const T& a, const T& b){
        return key_compare(key(a), key(b));
    };

    // <= for keys
    using key_t = decltype(key(std::declval<T>()));
    const auto key_leq = [key_compare](const key_t& ka, const key_t& kb){
        return (ka == kb) || key_compare(ka, kb);
    };

    // --- 1: SAMPLING ---
    std::vector<T> samples(a);
    {
        // pick samples
        std::random_device rd;
        std::mt19937 gen(rd()); // seed using random_device
        std::uniform_int_distribution<> dis(0, v.size() - 1);

        for(size_t i = 0; i < a; i++) {
            samples[i] = v[dis(gen)];
        }

        print_vector(ctx, samples, "picked samples");

        // send samples to master
        if(ctx.rank() != MASTER) {
            ctx.send(samples, MASTER);
        }
    }

    // --- 2: SPLITTERS ---
    std::vector<key_t> splitters(p-1);
    if(ctx.rank() == MASTER) {
        // receive samples
        std::vector<T> rcv_samples(a);
        for(size_t i = 0; i < p; i++) {
            if(i != MASTER) {
                ctx.recv(rcv_samples, a, i);
                for(const T& sample : rcv_samples) {
                    samples.push_back(sample);
                }
            }
        }
        print_vector(ctx, samples, "received samples");

        // sort samples
        std::sort(samples.begin(), samples.end(), item_compare);
        print_vector(ctx, samples, "sorted samples");

        // determine p-1 splitters (in a naive way... but good enough for now)
        for(size_t k = 0; k < p-1; k++) {
            splitters[k] = key(samples[k * a + 1]);
        }
        print_vector(ctx, splitters, "splitters");

        // broadcast splitters
        for(size_t i = 0; i < p; i++) {
            if(i != MASTER) {
                ctx.send(splitters, i);
            }
        }
    } else { // not MASTER
        // receive splitters
        ctx.recv(splitters, p-1, MASTER);
        print_vector(ctx, splitters, "received splitters");
    }
    // TODO: discard samples

    // --- 3: DISTRIBUTION ---
    std::vector<T> send_to_self;
    {
        // assign elements to buckets
        std::vector<std::vector<T>> outbox(p);
        for(const T& x : v) {
            const size_t bucket = lb_rank(splitters, key(x), key_compare);
            outbox[bucket].push_back(x);
        }

        // send buckets
        for(size_t i = 0; i < p; i++) {
            if(i == ctx.rank()) {
                send_to_self = outbox[i]; // "send to self"
            } else {
                ctx.send(outbox[i], i); // always send -- may be zero
            }
        }
    }

    // --- 4: RECEIVE AND SORT LOCALLY ---
    {
        // receive elems
        v.clear();

        std::vector<T> rbuf;
        for(size_t i = 0; i < p; i++) {
            if(i == ctx.rank()) {
                // "receive from self" - must happen in order for stable sort
                for(const T& x : send_to_self) {
                    v.push_back(x);
                }

                // TODO: discard send_to_self
            } else {
                auto result = ctx.probe<T>(i);
                ctx.recv(rbuf, result.size, i);

                for(const T& x : rbuf) {
                    v.push_back(x);
                }
            }
        }

        // sort elems
        std::stable_sort(v.begin(), v.end(), item_compare);
    }
}

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    std::vector<uint32_t> v;
    switch(ctx.rank()) {
        case 0: v = { 7, 5, 2, 1, 4, 1, 8, 9, 4, 9, 4, 2, 1, 3 }; break;
        case 1: v = { 4, 1, 3, 8, 4, 2, 1, 3, 6, 7, 7, 3, 4, 5 }; break;
        case 2: v = { 0, 6, 7, 9, 9, 1, 0, 5, 4, 1, 0, 2, 5, 4 }; break;
        case 3: v = { 3, 5, 2, 6, 0, 8, 3, 2, 7, 6, 8, 7, 5, 3 }; break;
    }

    print_vector(ctx, v, "input");

    ctx.synchronize();
    stable_sort(
        ctx,
        v,
        [](uint32_t x) -> uint32_t{return x & 1;},
        [](uint32_t a, uint32_t b){return a < b;},
        ctx.num_workers());

    ctx.synchronize();
    print_vector(ctx, v, "output");

    return 0;
}
