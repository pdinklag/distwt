#include <cstring>
#include <stdlib.h>
#include <iostream>
#include <tlx/cmdline_parser.hpp>
#include <tlx/string/format_si_iec_units.hpp>

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;
    cp.set_description("Performs a step-by-step test checking how much "
                       "consecutive memory can be allocated using malloc");
    cp.set_author("Patrick Dinklage <patrick.dinklage@udo.edu>");

    uint64_t max, step;
    bool write = false;
    cp.add_param_bytes("max", max, "The largest allocation to test.");
    cp.add_param_bytes("step", step, "The step size.");
    cp.add_flag('w', "write", write, "Also fill the allocated memory using "
                                     "memset.");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    if(step == 0) {
        std::cerr << "Step must be at least one byte!" << std::endl;
        return -1;
    }

    // test
    for(uint64_t size = step; size <= max; size += step) {
        std::cout << "malloc("
            << tlx::format_iec_units(size, 3) << "B) ... " << std::endl;

        void* p = malloc(size);

        if(p == nullptr) {
            std::cout << "    Failed!" << std::endl;
            return 1;
        } else {
            if(write) {
                std::cout << "    ... address is " << p
                    << ", writing ..." << std::endl;
                memset(p, 0, size);
                std::cout << "    Success!" << std::endl;
            } else {
                std::cout << "    Success (address is " << p << ")!" << std::endl;
            }

            free(p);
        }
    }
}
