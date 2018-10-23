#include <exception>
#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp> // debug
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/text.hpp>
#include <distwt/wt.hpp>
#include <distwt/binary_io.hpp>
#include <distwt/effective_alphabet.hpp>
#include <distwt/histogram.hpp>
#include <distwt/dia_compare.hpp>
#include <distwt/dia_prefix.hpp>

class wt_verification_failure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

using esym_index_t = std::pair<esym_t, size_t>;
inline std::ostream& operator<<(std::ostream& os, const esym_index_t& x) {
    os << "(" << x.first << ", " << x.second << ")";
    return os;
}

auto ReadNode(
    thrill::Context& ctx,
    WaveletTree& wt,
    const std::vector<size_t>& node_sizes,
    const size_t height,
    size_t node_id,
    size_t level) {

    const size_t sz = node_sizes[node_id-1];
    if(sz > 0) {
        const size_t lsh = height - 1ULL - level;

        return dia_prefix<esym_t>(wt.load_node_bv(node_id)
            .template FlatMap<esym_t>([lsh](const uint64_t& x, auto emit) {
                // expand bits to 64 esym_t values
                for(size_t i = 0; i < 64; ++i) {
                    bool b = (x & (1ULL << (63ULL - i)));
                    emit(esym_t(b) << lsh);
                }
            }), sz);
    } else {
        return thrill::api::Generate(ctx, 0, [](size_t){return esym_t(0);});
    }
}

thrill::DIA<esym_index_t> RecursiveDecode(
    thrill::DIA<esym_index_t> s,
    thrill::Context& ctx,
    WaveletTree& wt,
    const std::vector<size_t>& node_sizes,
    const size_t height,
    size_t node_id,
    size_t level) {

    auto zipped = s.Zip(
        ReadNode(ctx, wt, node_sizes, height, node_id, level),
        [](const esym_index_t& x, esym_t c){
            return esym_index_t(x.first | c, x.second);
        }).Cache();

    // DEBUG begin
    size_t sz = zipped.Keep().Size();
    if(ctx.my_rank() == 0) {
        LOG1 << "node_id = " << node_id << ", size = " << sz;
    }
    // DEBUG end

    if(level + 1 < height) {
        // recurse
        const size_t rsh = height - 1ULL - level;

        auto l = RecursiveDecode(
            zipped.Filter([rsh](const esym_index_t& x){
                return !bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, wt, node_sizes, height, 2ULL * node_id, level + 1);

        auto r = RecursiveDecode(
            zipped.Filter([rsh](const esym_index_t& x){
                return bool((x.first >> rsh) & 1ULL);
            }).Collapse(),
            ctx, wt, node_sizes, height, 2ULL * node_id + 1ULL, level + 1);

        return l.Concat(r).Cache();
    } else {
        return zipped.Cache();
    }
}

auto DecodeWTNodes(thrill::Context& ctx, const std::string& wtfile) {
    // load histogram
    WaveletTree wt(ctx, wtfile);
    auto hist = wt.load_histogram();

    // compute required information
    const size_t height = WaveletTree::height(hist);
    auto node_sizes = WaveletTree::node_sizes(hist);
    const size_t n = node_sizes[0];

    // decode
    auto xtext = thrill::api::Generate(ctx, n,
        [](size_t i){ return esym_index_t(0, i); }
    );

    return RecursiveDecode(xtext, ctx, wt, node_sizes, height, 1, 0)
        .Sort([](const esym_index_t& a, const esym_index_t& b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](const esym_index_t& x){
            // undo effective transformation
            return hist.entries[x.first].first;
        });
}

void Process(thrill::Context& ctx,
    std::string original,
    std::string wtfile) {

    // decode WT
    auto decoded = DecodeWTNodes(ctx, wtfile);

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, original);

    // compare raw and decoded texts as a means to verify the WT
    const size_t diff = dia_compare<rawsym_t>(rawtext, decoded);

    // output result on first worker
    if(ctx.my_rank() == 0) {
        if(diff == 0) {
            std::cout << "WT verification succeeded!" << std::endl;
        } else {
            throw wt_verification_failure(
                std::string("WT verification FAILED: diff=") +
                std::to_string(diff));
        }
    }
}

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <original> <wt>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], argv[2]);
    });
}
