#include <iostream>
#include <tuple>
#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/write_binary.hpp>

#include <distwt/text.hpp>
#include <distwt/wt.hpp>
#include <distwt/util.hpp>

#include <distwt/binary_io.hpp>
#include <distwt/histogram.hpp>
#include <distwt/effective_alphabet.hpp>

void Process(
    thrill::Context& ctx,
    std::string input,
    size_t input_size,
    std::string output) {

    // load raw text
    auto rawtext = thrill::api::ReadBinary<rawsym_t>(ctx, input).Cache();

    // compute histogram
    Histogram hist(rawtext);

    // compute effective alphabet
    EffectiveAlphabet ea(hist);

    // transform text
    auto etext = ea.transform(rawtext).Cache();

    // TODO: construct WT
    // WaveletTree wt(ctx, output);
    const size_t height = WaveletTree::height(hist);

    etext.template GroupToIndex<std::vector<bool>>(
        // compute ID of node on next level in which the character will "land"
        [height](esym_t x){ return size_t(x >> (height-1ULL)) & 1ULL; },
        // group
        [height](auto& iter, const size_t& key) mutable {
            std::vector<bool> bv;
            while (iter.HasNext()) {
                bv.emplace_back(bool((iter.Next() >> (height-2ULL)) & 1ULL));
            }
            return bv;
        },
        2ULL // amount of indices
    ).Print("result");
}

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <input> <output>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Process(ctx, argv[1], util::file_size(argv[1]), argv[2]);
    });
}
