#include <cassert>
#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/integer_log2.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/wm.hpp>

#include <distwt/mpi/result.hpp>

//#define DBG_CONCAT 1

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

    // Build wavelet matrix
    auto wm = WaveletMatrix(hist,
    [&](WaveletMatrix::bits_t& bits, WaveletMatrix::z_t& z, const WaveletMatrixBase& wm){

        const size_t height = wm.height();

        bits.resize(height);

        #if DBG_CONCAT
        ctx.cout() << "size per worker: " << input.size_per_worker()
                   << ", local_num: " << local_num
                   << std::endl;
        ctx.synchronize();
        #endif

        // allocate two buffers - 0 and 1
        std::array<std::vector<esym_t>, 2> buffer;

        std::vector<uint64_t*> msg_headers;
        for(size_t level = 0; level < height; level++) {
            const int tag = int(level);
            ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

            const size_t num_nlevel_nodes = 1ULL << (level+1);
            const size_t first_nlevel_node = num_nlevel_nodes;

            // construct bit vector
            auto& level_bits = bits[level];
            level_bits.resize(local_num);

            size_t local_z = 0;
            size_t glob_z;

            auto reduce_z = [&](){
                ctx.all_reduce(&local_z, &glob_z, 1);

                #ifdef DBG_CONCAT
                ctx.cout_master() << "z = " << glob_z << std::endl;
                #endif

                if(ctx.rank() == 0) {
                    z[level] = glob_z;
                }
            };

            const size_t rsh = height - 1 - level;
            if(level+1 == height) {
                // this is the last level, build only the bit vector
                for(size_t i = 0; i < local_num; i++) {
                    const bool b = (etext[i] >> rsh) & 1;
                    level_bits[i] = b;
                    if(!b) ++local_z;
                }

                reduce_z();
            } else { // if level+1 < height
                // while building the bit vector, also fill the buffers
                for(size_t i = 0; i < local_num; i++) {
                    const esym_t x = etext[i];
                    const bool b = (x >> rsh) & 1;
                    
                    level_bits[i] = b;
                    if(!b) ++local_z;
                    buffer[b].push_back(x);
                }

                // distribute buffers

                // reduce Z
                reduce_z();

                // compute buffer size prefix sums
                std::vector<size_t> buffer_offs(2);
                buffer_offs[0] = buffer[0].size();
                buffer_offs[1] = buffer[1].size();
                ctx.ex_scan(buffer_offs);

                // send buffers away
                size_t glob_offs = 0;
                for(size_t b = 0; b <= 1; b++) {
                  if(buffer[b].size() > 0) {

                    const size_t glob_buffer_offs = glob_offs + buffer_offs[b];

                    #ifdef DBG_CONCAT
                    ctx.cout() << "processing buffer " << b
                        << " with glob_buffer_offs = " << glob_buffer_offs
                        << " (glob_offs = " << glob_offs
                        << ", prefix sum = " << buffer_offs[b] << ")"
                        << std::endl;
                    #endif

                    // target of first buffer item
                    const size_t target1 =
                        glob_buffer_offs / input.size_per_worker();

                    // target of last buffer item
                    const size_t glob_last =
                        glob_buffer_offs + buffer[b].size() - 1;
                    const size_t target2 =
                        glob_last / input.size_per_worker();

                    #ifdef DBG_CONCAT
                    ctx.cout() << "substring [" << glob_buffer_offs
                        << ", " << glob_last << "] for buffer " << b
                        << " goes to target1=" << target1
                        << " and target2=" << target2
                        << std::endl;
                    #endif

                    if(target1 == target2) {
                        // whole buffer goes to target
                        const size_t target = target1;

                        // send one message
                        auto header = new uint64_t[2];
                        header[0] = glob_buffer_offs;
                        header[1] = buffer[b].size();
                        msg_headers.push_back(header);

                        ctx.isend(header, 2, target, tag);
                        ctx.isend(buffer[b], target, tag);

                        #ifdef DBG_CONCAT
                        ctx.cout()
                            << "(single) send " << buffer[b].size() << " at "
                            << glob_buffer_offs << " (local 0)"
                            << " to " << target
                            << std::endl;
                        #endif
                    } else {
                        // bucket can go to at most two different targets!
                        assert(target1 + 1 == target2);

                        const size_t glob_first2 =
                            target2 * input.size_per_worker();
                        assert(glob_first2 > glob_buffer_offs);
                        assert(glob_first2 <= glob_last);

                        const size_t size1 = glob_first2 - glob_buffer_offs;
                        const size_t size2 = glob_last - glob_first2 + 1;

                        // message to first
                        {
                            auto header1 = new uint64_t[2];
                            header1[0] = glob_buffer_offs;
                            header1[1] = size1;
                            msg_headers.push_back(header1);

                            #ifdef DBG_CONCAT
                            ctx.cout()
                                << "(#1) send " << size1 << " at "
                                << glob_buffer_offs << " (local 0)"
                                << " to " << target1
                                << std::endl;
                            #endif

                            ctx.isend(header1, 2, target1, tag);
                            ctx.isend(buffer[b].data(), size1, target1, tag);
                        }

                        // message to second
                        {
                            auto header2 = new uint64_t[2];
                            header2[0] = glob_first2;
                            header2[1] = size2;
                            msg_headers.push_back(header2);

                            ctx.isend(header2, 2, target2, tag);
                            ctx.isend(
                                buffer[b].data() + size1, size2, target2, tag);

                            #ifdef DBG_CONCAT
                            ctx.cout()
                                << "(#2) send " << size2 << " at "
                                << glob_first2 << " (local " << size1
                                << ") to " << target2
                                << std::endl;
                            #endif
                        }
                    }
                  } else {
                        #ifdef DBG_CONCAT
                        ctx.cout() << "skipping buffer " << b
                            << " because it is empty"
                            << std::endl;
                        #endif
                  }
                  glob_offs += glob_z;
                }

                // receive substrings until text is filled locally
                {
                    const size_t expect = local_num;
                    size_t num_received = 0;
                    while(num_received < expect) {
                        // probe for message (blocking)
                        auto result = ctx.template probe<uint64_t>(tag);

                        // receive header (global interval)
                        uint64_t* rheader = new uint64_t[result.size];
                        ctx.recv(rheader, result.size, result.sender, tag);

                        const size_t glob_offs = rheader[0];
                        const size_t size = rheader[1];
                        const size_t local_offs =
                            glob_offs % input.size_per_worker();

                        #ifdef DBG_CONCAT
                        ctx.cout()
                            << "receive " << size << " at "
                            << glob_offs << " (local " << local_offs
                            << ") from " << result.sender
                            << std::endl;
                        #endif

                        assert(local_offs < local_num);
                        assert(local_offs + size <= local_num);

                        // receive substring
                        ctx.recv(etext.data() + local_offs, size,
                            result.sender, tag);

                        num_received += size;

                        #ifdef DBG_CONCAT
                        ctx.cout()
                            << "got " << num_received << " of " << expect
                            << std::endl;
                        #endif

                        // clean message buffer
                        delete[] rheader;
                    }
                    assert(num_received == expect);
                }

                // synchronize before cleaning!
                ctx.synchronize();

                // clean up
                buffer[0].clear();
                buffer[1].clear();
                
                for(auto header : msg_headers) {
                    delete[] header;
                }
                msg_headers.clear();
            }
        }
    });

    time.construct = dt();
    time.merge = 0;

    // write to disk if needed
    if(output.length() > 0) {
        ctx.synchronize();
        ctx.cout_master() << "Writing WT to disk ..." << std::endl;

        if(ctx.rank() == 0) {
            hist.save(output + "." + WaveletMatrixBase::histogram_extension());
            wm.save_z(output + "." + WaveletMatrixBase::z_extension());
        }
        
        wm.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-wm-concat", ctx, input, wm.sigma(), time);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
    return 0;
}
