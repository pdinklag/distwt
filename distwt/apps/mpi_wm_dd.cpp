#include <string>
#include <vector>

#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/common/wm_sequential.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/bit_vector.hpp>
#include <distwt/mpi/wm.hpp>

#include <distwt/mpi/result.hpp>

int main(int argc, char** argv) {
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

    Result::Time time;
    double t0 = ctx.time();

    auto dt = [&](){
        const double t = ctx.time();
        const double dt = t - t0;
        t0 = t;
        return dt;
    };

    // Determine input partition
    FilePartitionReader input(ctx, input_filename, prefix);
    const size_t local_num = input.local_num();

    if(rdbufsize == 0) {
        rdbufsize = local_num;
    }

    if(local_filename.length() > 0) {
        // Extract local part
        ctx.cout_master() << "Extract partition to "
            << local_filename << " ..." << std::endl;

        input.extract_local(local_filename, rdbufsize);

        // Synchronize
        ctx.cout_master() << "Synchronizing ..." << std::endl;
        ctx.synchronize();
    }

    time.input = dt();

    // Compute histogram
    ctx.cout_master() << "Compute histogram ..." << std::endl;
    Histogram hist(ctx, input, rdbufsize);

    time.hist = dt();

    // Compute effective alphabet
    EffectiveAlphabet ea(hist);

    // Transform text and cache in RAM
    ctx.cout_master() << "Compute effective transformation ..." << std::endl;
    std::vector<esym_t> etext(local_num);
    {
        size_t i = 0;
        ea.transform(input, [&](esym_t x){ etext[i++] = x; }, rdbufsize);
    }

    time.eff = dt();

    // local construction
    ctx.cout_master() << "Compute WM ..." << std::endl;
    auto wm = WaveletMatrix(hist,
    [&](WaveletMatrix::bits_t& bits,
        WaveletMatrix::z_t& z,
        const WaveletMatrixBase& wm) {

        bits.resize(wm.height());
        wm_pc(wm, bits, z, etext);
    });

    // Clean up
    etext.clear();
    etext.shrink_to_fit();

    // Synchronize
    ctx.cout_master() << "Done. Synchronizing ..." << std::endl;
    ctx.synchronize();

    time.construct = dt();

    /*
    // Convert to level-wise representation
    WaveletMatrix wm = wt_nodes.merge(ctx, input, hist, true);
    time.merge = dt();
    */

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WM to disk ..." << std::endl;

        hist.save(output + "." + WaveletTreeBase::histogram_extension());
        wm.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-wm-dd", ctx, input, wm.sigma(), time);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
    return 0;
}
