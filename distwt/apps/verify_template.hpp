#include <exception>
#include <iostream>

#include <distwt/common/wt.hpp>

#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <distwt/thrill/dia_compare.hpp>

#include <distwt/thrill/text.hpp>
#include <distwt/thrill/histogram.hpp>

class wt_verification_failure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

template<typename wt_t>
void Verify(thrill::Context& ctx,
    std::string original,
    std::string wtfile) {

    // load WT
    Histogram hist(wtfile + "." + WaveletTreeBase::histogram_extension());
    wt_t wt(hist, ctx, wtfile);

    // decode WT
    auto decoded = wt.decode(ctx, hist);

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
