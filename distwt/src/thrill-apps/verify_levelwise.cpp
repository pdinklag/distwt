#include <exception>
#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/common/binary_io.hpp>
#include <distwt/common/util.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/wt.hpp>
#include <distwt/thrill/effective_alphabet.hpp>
#include <distwt/thrill/histogram.hpp>
#include <distwt/thrill/dia_compare.hpp>
#include <distwt/thrill/dia_prefix.hpp>

class wt_verification_failure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

auto DecodeWT(thrill::Context& ctx, const std::string& wtfile) {
    // indexed symbol
    using esym_index_t = std::pair<esym_t, size_t>;

    // load auxiliary data
    WaveletTree wt(ctx, wtfile);
    auto hist = wt.load_histogram();
    const size_t n = hist.text_length();

    // construct indexed sequence of 0 symbols
    auto xtext = thrill::api::Generate(ctx, n, [](size_t i){
        return esym_index_t(0, i);}
    );

    // reconstruct symbols from bit vector level by level
    const size_t height = WaveletTree::height(hist);
    for(size_t level = 0; level < height; level++) {
        const size_t lsh = height - 1ULL - level;

        xtext = dia_prefix<esym_t>(wt.load_level_bv(level)
            .template FlatMap<esym_t>([lsh](const uint64_t& x, auto emit) {
                // expand bits to 64 boolean values
                for(size_t i = 0; i < 64; ++i) {
                    bool b = (x & (1ULL << (63ULL - i)));
                    emit(esym_t(b) << lsh);
                }
            }), n)
            .Zip(xtext, [lsh](esym_t c, esym_index_t x) {
                // OR symbols using current vector
                return esym_index_t(x.first | c, x.second);
            })
            .SortStable([lsh](esym_index_t a, esym_index_t b){
                // stably reorder according to newest bit
                return (a.first >> lsh) < (b.first >> lsh);
            })
            .Collapse();
    }

    // restore original text
    return xtext
        .Sort([](esym_index_t a, esym_index_t b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](esym_index_t x){ // TODO: capture hist by reference?
            // undo effective transformation
            return hist.entries[x.first].first;
        });
}

void Process(thrill::Context& ctx,
    std::string original,
    std::string wtfile) {

    // decode WT
    auto decoded = DecodeWT(ctx, wtfile);

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
