#include <algorithm>
#include <cassert>
#include <cmath>
#include <map>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

#include <tlx/cmdline_parser.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/mpi_type.hpp>

template<typename T>
void print_vector(MPIContext& ctx, std::vector<T>& v, std::string name) {
    auto& out = ctx.cout();
    out << name << ": [ ";
    for(const T& x : v) out << x << " ";
    out << "]" << std::endl;
}

constexpr size_t MASTER = 0;
using sortkey_t = size_t;

// find rank of greatest key <= "it"
// may go one beyond the right bound!
template<typename key_compare_t>
size_t lb_rank(
    const std::vector<sortkey_t>& keys,
    const sortkey_t& it,
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
    key_extractor_t key, // T -> sortkey_t
    key_compare_t key_compare, // (sortkey_t, sortkey_t) -> bool
    const size_t a // oversampling factor
) {
    static_assert(
        std::is_same<decltype(key(std::declval<T>())), sortkey_t>::value,
        "key type must be sortkey_t!");

    const size_t p = ctx.num_workers();

    // compare elements using their keys
    const auto elem_compare = [key,key_compare](const T& a, const T& b){
        return key_compare(key(a), key(b));
    };

    // <= for keys
    const auto key_leq = [key_compare](const sortkey_t& ka, const sortkey_t& kb){
        return (ka == kb) || key_compare(ka, kb);
    };

    // --- 1: SAMPLING ---
    std::vector<T> samples(a);
    {
        // pick samples
        std::random_device rd;
        std::mt19937 gen(rd()); // seed using random_device
        //std::mt19937 gen(112113); // seed
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
    std::vector<sortkey_t> splitters;
    std::vector<size_t> key_count;
    size_t num_distinct_keys;

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
        std::sort(samples.begin(), samples.end(), elem_compare);
        print_vector(ctx, samples, "sorted samples");

        // find number of distinct keys
        num_distinct_keys = 0;
        {
            sortkey_t prev_key;
            for(const T& x : samples) {
                sortkey_t kx = key(x);
                if(num_distinct_keys == 0 || key_compare(prev_key, kx)) {
                    // first key or GREATER than previous
                    ++num_distinct_keys;
                } else {
                    // EQUAL to previous (splitters are sorted!)
                }
                prev_key = kx;
            }
        }

        // broadcast number of distinct keys
        for(size_t i = 0; i < p; i++) {
            if(i != MASTER) {
                ctx.send(&num_distinct_keys, 1, i);
            }
        }
        ctx.cout_master() << "num_distinct_keys: " << num_distinct_keys
                          << std::endl;

        if(num_distinct_keys <= p) {
            // SMALL KEY SET
            // create histogram of keys -- time: O(ap)
            splitters.resize(num_distinct_keys);
            key_count.resize(num_distinct_keys);

            sortkey_t prev_key;
            size_t key_rank = 0;
            for(const T& x : samples) {
                sortkey_t kx = key(x);
                if(num_distinct_keys == 0) {
                    // first key
                    splitters[0] = kx;
                    key_count[0] = 1;
                } else if(key_compare(prev_key, kx)) {
                    // new key
                    ++key_rank;
                    splitters[key_rank] = kx;
                    key_count[key_rank] = 1;
                } else {
                    // same key
                    ++key_count[key_rank];
                }
                prev_key = kx;
            }

            print_vector(ctx, splitters, "keys");
            print_vector(ctx, key_count, "key counts");

            // broadcast
            for(size_t i = 0; i < p; i++) {
                if(i != MASTER) {
                    ctx.send(splitters, i);
                    ctx.send(key_count, i);
                }
            }
        } else {
            // CLASSIC SSS-SORT
            // determine p-1 splitters (good enough?)
            splitters.resize(p-1);
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
        }
    } else { // not MASTER
        // receive number of distinct keys
        ctx.recv(&num_distinct_keys, 1, MASTER);

        if(num_distinct_keys <= p) {
            // SMALL KEY SET
            // receive keys and histogram
            ctx.recv(splitters, num_distinct_keys, MASTER);
            ctx.recv(key_count, num_distinct_keys, MASTER);
            print_vector(ctx, splitters, "received keys");
            print_vector(ctx, key_count, "received key counts");
        } else {
            // CLASSIC SSS-SORT
            // receive splitters
            ctx.recv(splitters, p-1, MASTER);
            print_vector(ctx, splitters, "received splitters");
        }
    }
    // TODO: discard samples

    // --- 3: DISTRIBUTION ---
    const size_t m = num_distinct_keys;
    std::vector<T> send_to_self;
    {
        std::vector<std::vector<T>> outbox(p);

        if(m <= p) {
            // SMALL KEY SET

            // distribute workers
            size_t assigned = 0;
            std::vector<size_t> workers(m);
            for(size_t k = 0; k < m; k++) {
                // workers = r * p = (# / p*a) * p = # / a
                const float num_workers_f = (float)key_count[k] / (float)a;
                const size_t num_workers = size_t(std::round(num_workers_f));
                workers[m] = num_workers;
            }

            // assigned may be less or higher than p
            //
            // we round off by adding where the number of workers is the lowest
            // or reducing where the number of workers is the highest
            // this is far from optimal (statistically), but doing it right
            // is a LOT more complicated
            while(assigned < p) {
                auto it = std::min_element(workers.begin(), workers.end());
                ++(*it);
                ++assigned;
            }
            while(assigned > p) {
                auto it = std::max_element(workers.begin(), workers.end());
                assert(*it > 1);
                --(*it);
                --assigned;
            }
            assert(assigned == p);

            std::map<
                sortkey_t,
                std::pair<size_t, size_t>,
                key_compare_t> assignment;

            size_t next_worker = 0;
            for(size_t k = 0; k < m; k++) {
                const size_t num_workers = workers[k];
                assert(num_workers > 0);

                ctx.cout_master()
                    << "workers for " << splitters[k]
                    << ": " << workers[k]
                    << " -- [" << next_worker << ", "
                    << next_worker+num_workers-1 << "]"
                    << std::endl;

                assignment.emplace(splitters[k],
                    std::make_pair(next_worker, next_worker+num_workers-1));

                next_worker += num_workers;
            }

            // assign elements to buckets
            const float rel_rank = (float)ctx.rank() / (float)ctx.num_workers();
            for(const T& x : v) {
                // key set is based on samples and it may still be that
                // there are more keys than detected -- use lower_bound
                std::pair<size_t, size_t> range;
                const auto it = assignment.lower_bound(key(x));
                if(it != assignment.end()) {
                    range = it->second;
                } else {
                    auto last = assignment.rbegin();
                    range = (++last)->second;
                }

                const size_t j = std::min(p-1, size_t(
                    range.first +
                    std::round(rel_rank * (range.second - range.first))));

                assert(j < p);
                ctx.cout()
                    << "sending " << x << " (key: " << key(x) << ") to "
                    << j << std::endl;
                outbox[j].push_back(x);
            }
        } else {
            // CLASSIC SSS-SORT
            // assign elements to buckets
            for(const T& x : v) {
                const size_t bucket = lb_rank(splitters, key(x), key_compare);
                outbox[bucket].push_back(x);
            }
        }

        // send buckets
        send_to_self = outbox[ctx.rank()]; // "send to self"
        for(size_t i = 0; i < p; i++) {
            if(i != ctx.rank()) {
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
        std::stable_sort(v.begin(), v.end(), elem_compare);
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