#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/integer_log2.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/stable_sort.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

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

    // Convert to level-wise representation
    auto wt = WaveletTreeLevelwise(hist,
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        const size_t height = wt.height();
        bits.resize(height);

        for(size_t level = 0; level < height; level++) {
            ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

            // construct bit vector
            auto& level_bits = bits[level];
            level_bits.resize(etext.size());

            const size_t rsh = height - 1 - level;
            for(size_t i = 0; i < etext.size(); i++) {
                level_bits[i] = bool((etext[i] >> rsh) & 1);
            }

            if(level+1 < height) {
                // stably sort according to bit prefix
                ctx.cout_master() << "sort ..." << std::endl;
                stable_sort(ctx, etext,
                    [rsh](esym_t x){return sortkey_t(x >> rsh);},
                    [](sortkey_t a, sortkey_t b){return a < b;},
                    ctx.num_workers());
            }
        }
    });

    time.construct = dt();
    time.merge = 0;

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WT to disk ..." << std::endl;

        hist.save(output + "." + WaveletTreeBase::histogram_extension());
        wt.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-dsplit", ctx, input, wt.sigma(), time);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
    return 0;
}
