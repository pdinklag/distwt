#include <iostream>
#include <tuple>

#include <tlx/cmdline_parser.hpp>

#include <thrill/api/dia.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>

#include <distwt/thrill/force.hpp>

#include <thrill/common/stats_timer.hpp>

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    // launch Thrill process
    return thrill::Run([&](thrill::Context& ctx) {
        thrill::common::StatsTimer timer(true);

        // load text
        auto text = thrill::api::ext::Force(
            thrill::api::ReadBinary<uint8_t>(ctx, input_filename).Cache());

        const double t1 = timer.SecondsDouble();
        LOG1 << "t1=" << t1;

        timer.Reset();
        text.Map([&](uint8_t c){return c;}).Size();
        const double t2 = timer.SecondsDouble();
        LOG1 << "t2=" << t2;
    });
}
