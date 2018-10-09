#include <iostream>
#include <tuple>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/zip.hpp>

#include <distwt/text.hpp>
#include <distwt/wt.hpp>
#include <distwt/binary_io.hpp>
#include <distwt/effective_alphabet.hpp>
#include <distwt/histogram.hpp>
#include <distwt/dia_compare.hpp>

thrill::DIA<rawsym_t> DecodeWT(
    thrill::Context& ctx,
    const wt_bits_t& wt_bits,
    const size_t original_size,
    const Histogram& hist) {

    using esym_index_t = std::pair<esym_t, size_t>;

    // reconstruct symbols from bit vector
    auto xtext = thrill::api::Generate(ctx, original_size, [](size_t i){
        // construct indexed sequence of 0 symbols
        return esym_index_t(0, i);}
    );

    const size_t wt_height = wt_bits.size();
    for(size_t level = 0; level < wt_height; level++) {
        const size_t lsh = wt_height - 1 - level;

        xtext = wt_bits[level]
            .template FlatMap<bool>([](const uint64_t& x, auto emit) {
                // expand bits to 64 boolean values
                for(size_t i = 0; i < 64; ++i) {
                    emit((x & (1ULL << (63ULL - i))));
                }
            })
            .template FlatWindow<bool>(thrill::api::DisjointTag, 64,
            [original_size](
                size_t index,
                const std::vector<bool>& rb,
                auto emit) {

                // cut off bits beyond original input size
                for(size_t i = 0; i < rb.size(); i++) {
                    if(index++ >= original_size) {
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
    std::string wtfile,
    const size_t original_size,
    const Histogram& hist) {

    // compute height of the WT
    const size_t sigma = hist.size();
    const size_t wt_height = tlx::integer_log2_ceil(sigma-1);

    // load WT levels
    wt_bits_t wt_bits;
    for(size_t i = 0; i < wt_height; i++) {
        wt_bits.emplace_back(thrill::api::ReadBinary<uint64_t>(
            ctx, wtfile + "*.lv_" + std::to_string(i+1)));
    }

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, original);

    // decode WT
    auto decoded = DecodeWT(ctx, wt_bits, original_size, hist);

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

    // load histogram and original file size
    std::string wtfile(argv[2]);
    Histogram hist(wtfile + ".hist");

    size_t original_size;
    {
        binary::FileReader r(wtfile + ".size");
        original_size = r.template read<size_t>();
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], wtfile, original_size, hist);
    });
}
