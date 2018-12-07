#include "verify_template.hpp"
#include <distwt/thrill/wt_levelwise.hpp>

int main(int argc, const char** argv) {
    // basic argument parsing
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <original> <wt>" << std::endl;
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        Verify<WaveletTreeLevelwise>(ctx, argv[1], argv[2]);
    });
}
