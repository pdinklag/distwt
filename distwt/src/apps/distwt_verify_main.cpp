#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/text.hpp>
#include <distwt/wt.hpp>
#include <distwt/binary_io.hpp>
#include <distwt/effective_alphabet.hpp>
#include <distwt/histogram.hpp>
#include <distwt/dia_compare.hpp>

thrill::DIA<rawsym_t> DecodeWT(thrill::Context& ctx, const WaveletTree& wt) {
    using esym_index_t = std::pair<esym_t, size_t>;

    // reconstruct symbols from bit vector
    const size_t n = wt.text_length();
    auto xtext = thrill::api::Generate(ctx, n, [](size_t i){
        // construct indexed sequence of 0 symbols
        return esym_index_t(0, i);}
    );

    const size_t wt_height = wt.height();
    for(size_t level = 0; level < wt_height; level++) {
        const size_t lsh = wt_height - 1 - level;

        xtext = wt.bits(level)
            .template FlatMap<bool>([](const uint64_t& x, auto emit) {
                // expand bits to 64 boolean values
                for(size_t i = 0; i < 64; ++i) {
                    emit((x & (1ULL << (63ULL - i))));
                }
            })
            .template FlatWindow<bool>(thrill::api::DisjointTag, 64,
            [n](size_t index, const std::vector<bool>& rb, auto emit) {

                // cut off bits beyond original input size
                for(size_t i = 0; i < rb.size(); i++) {
                    if(index++ >= n) {
                        break;
                    } else {
                        emit(rb[i]);
                    }
                }
            })
            .Zip(xtext, [lsh](bool bit, esym_index_t x) {
                // OR symbols using current vector
                return esym_index_t(x.first | (bit << lsh), x.second);
            })
            .SortStable([lsh](esym_index_t a, esym_index_t b){
                // stably reorder according to newest bit
                return (a.first >> lsh) < (b.first >> lsh);
            })
            .Execute() // do it NOW
            .Collapse();
    }

    // restore original text
    const auto& hist = wt.histogram();
    auto text = xtext
        .Sort([](esym_index_t a, esym_index_t b){
            // sort back by original index
            return a.second < b.second;
        })
        .Map([hist](esym_index_t x){
            // undo effective transformation
            return hist.entries[x.first].first;
        });

    return text.Cache();
}

void Process(thrill::Context& ctx,
    std::string original,
    std::string wtfile) {

    // load WT
    WaveletTree wt(ctx, wtfile);

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, original);

    // decode WT
    auto decoded = DecodeWT(ctx, wt);

    // compare raw and decoded texts as a means to verify the WT
    const size_t diff = dia_compare(rawtext, decoded);

    // output result on first worker
    if(ctx.my_rank() == 0) {
        if(diff == 0) {
            std::cout <<
                "WT verification succeeded!" << std::endl;
        } else {
            std::cout <<
                "WT verification FAILED (diff = " << diff << ")!" << std::endl;
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
