#pragma once

#include <tlx/cmdline_parser.hpp>
#include <distwt/mpi/context.hpp>

template<typename mpi_app_t>
int mpi_launch(int argc, char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;

    size_t rdbufsize = 0; // default to local input size
    cp.add_bytes('r', "rbuf", rdbufsize, "File read buffer size.");

    std::string local_filename("");
    cp.add_string('l', "local", local_filename, "Name of local part file.");

    std::string output("");
    cp.add_string('o', "output", output, "Name of output file.");

    size_t prefix = SIZE_MAX; // default to whole file
    cp.add_bytes('p', "prefix", prefix, "Only process prefix of input file.");

    std::string input_filename; // required
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Init MPI
    MPIContext ctx(&argc, &argv);

    // start
    mpi_app_t::template start<uint8_t>(
        ctx, input_filename, prefix, rdbufsize, local_filename, output);

    return 0;
}
