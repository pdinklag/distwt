/*
    This is no longer really needed, because all apps constructing the
    nodebased wavelet tree have been extended by a merge step to compute
    the levelwise wavelet tree instead.
*/

#include "verify_template.hpp"
#include <distwt/thrill/wt_nodebased.hpp>

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <original> <wt>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Verify<WaveletTreeNodebased>(ctx, argv[1], argv[2]);
    });
}
