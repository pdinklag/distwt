#include "mpi_launcher.hpp"

#include <cassert>
#include <memory>
#include <vector>

#include <tlx/cmdline_parser.hpp>
#include <tlx/math/integer_log2.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>
#include <distwt/mpi/mpi_max.hpp>

#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/wm.hpp>

#include <distwt/mpi/result.hpp>

//#define DBG_CONCAT 1

template<typename sym_t>
class DummyHistogram : public Histogram<sym_t> {
private:
    size_t m_size;
    
public:
    inline DummyHistogram(size_t size) : m_size(size) {
    }

    virtual inline size_t size() const override {
        return m_size;
    }
};

class mpi_wm_concat {
public:

template<typename sym_t>
static void start(
    MPIContext& ctx,
    const std::string& input_filename,
    const size_t prefix,
    const size_t in_rdbufsize,
    const std::string& local_filename,
    const bool eff_input,
    const std::string& output) {

    Result::Time time;
    double t0 = ctx.time();

    auto dt = [&](){
        const double t = ctx.time();
        const double dt = t - t0;
        t0 = t;
        return dt;
    };

    // Determine input partition
    FilePartitionReader<sym_t> input(ctx, input_filename, prefix);
    const size_t local_num = input.local_num();
    const size_t rdbufsize = (in_rdbufsize > 0) ? in_rdbufsize : local_num;

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

    std::vector<sym_t> etext;
    std::unique_ptr<Histogram<sym_t>> hist;
    
    if(eff_input) {
        ctx.cout_master() << "Skipping histogram computation!" << std::endl;
        time.hist = 0;

        // Load input and find maximum symbol
        ctx.cout_master() << "Loading input ..." << std::endl;
        sym_t local_max = 0;
        
        etext.reserve(local_num);
        input.process_local([&](const sym_t x){
            local_max = std::max(x, local_max);
            etext.push_back(x);
        }, rdbufsize);

        // Reduce maximum symbol
        sym_t glob_max;
        ctx.all_reduce(&local_max, &glob_max, 1, mpi_max<sym_t>::op());

        // Create dummy histogram instance
        hist = std::make_unique<DummyHistogram<sym_t>>(glob_max);

        // Update timer
        time.eff = dt();
    } else {
        // Compute histogram
        ctx.cout_master() << "Compute histogram ..." << std::endl;
        hist = std::make_unique<Histogram<sym_t>>(ctx, input, rdbufsize);

        time.hist = dt();

        // Compute effective alphabet
        EffectiveAlphabet<sym_t> ea(*hist);

        // Transform text and cache in RAM
        ctx.cout_master() << "Compute effective transformation ..." << std::endl;
        etext.resize(local_num);
        {
            size_t i = 0;
            ea.transform(input, [&](sym_t x){ etext[i++] = x; }, rdbufsize);
        }

        time.eff = dt();
    }

    // Build wavelet matrix
    auto wm = WaveletMatrix(*hist,
    [&](WaveletMatrix::bits_t& bits, WaveletMatrix::z_t& z, const WaveletMatrixBase& wm){

        const size_t height = wm.height();

        bits.resize(height);

        #if DBG_CONCAT
        ctx.cout() << "size per worker: " << input.size_per_worker()
                   << ", local_num: " << local_num
                   << std::endl;
        ctx.synchronize();
        #endif

        // allocate buffer and pointers into it
        std::vector<sym_t> buffer(local_num);
        size_t num0;

        uint64_t msg_header1[2][2], msg_header2[2][2], rheader[2];
        //std::vector<uint64_t*> msg_headers;
        for(size_t level = 0; level < height; level++) {
            const int tag = int(level);
            ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

            const size_t num_nlevel_nodes = 1ULL << (level+1);
            const size_t first_nlevel_node = num_nlevel_nodes;

            if(level+1 == height) {
                // we don't need the buffer anymore
                buffer.clear();
                buffer.shrink_to_fit();
            }

            // construct bit vector
            auto& level_bits = bits[level];
            level_bits.resize(local_num);

            size_t glob_z;

            auto reduce_z = [&](){
                ctx.all_reduce(&num0, &glob_z, 1);

                #ifdef DBG_CONCAT
                ctx.cout_master() << "z = " << glob_z << std::endl;
                #endif

                z[level] = glob_z;
            };

            const size_t rsh = height - 1 - level;
            if(level+1 == height) {
                // this is the last level, build only the bit vector
                size_t num1 = 0;
                for(size_t i = 0; i < local_num; i++) {
                    const bool b = (etext[i] >> rsh) & 1;
                    level_bits[i] = b;
                    num1 += b;
                }

                num0 = local_num - num1;
                reduce_z();
            } else { // if level+1 < height

                // construct the bit vector and fill buffer
                // we fill the 0-buffer from left to right using num0
                // and the 1-buffer from right to left using p1
                num0 = 0;
                size_t p1 = local_num - 1;

                for(size_t i = 0; i < local_num; i++) {
                    const sym_t x = etext[i];
                    const bool b = (x >> rsh) & 1;

                    level_bits[i] = b;
                    if(b) {
                        buffer[p1--] = x;
                    } else {
                        buffer[num0++] = x;
                    }
                }

                assert(num0 == p1+1); // buffer must be full
                const size_t num1 = local_num - num0;

                // reverse 1-buffer in-place
                {
                    size_t i = p1+1;
                    size_t j = local_num - 1;

                    while(j > i) {
                        std::swap(buffer[i++], buffer[j--]);
                    }
                }

                // reduce Z
                reduce_z();

                // distribute buffers
                std::array<size_t, 2> buffer_size = { num0, num1 };
                std::array<const sym_t*, 2> buffer_ptr = { buffer.data(), buffer.data() + num0 };

                // compute buffer size prefix sums
                std::vector<size_t> buffer_offs(2);
                buffer_offs[0] = num0;
                buffer_offs[1] = num1;
                ctx.ex_scan(buffer_offs);

                // send buffers away
                size_t glob_offs = 0;
                for(size_t b = 0; b <= 1; b++) {
                  if(buffer_size[b] > 0) {

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
                        glob_buffer_offs + buffer_size[b] - 1;
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
                        msg_header1[b][0] = glob_buffer_offs;
                        msg_header1[b][1] = buffer_size[b];

                        ctx.isend(msg_header1[b], 2, target, tag);
                        ctx.isend(buffer_ptr[b], buffer_size[b], target, tag);

                        #ifdef DBG_CONCAT
                        ctx.cout()
                            << "(single) send " << buffer_size[b] << " at "
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
                            msg_header1[b][0] = glob_buffer_offs;
                            msg_header1[b][1] = size1;

                            #ifdef DBG_CONCAT
                            ctx.cout()
                                << "(#1) send " << size1 << " at "
                                << glob_buffer_offs << " (local 0)"
                                << " to " << target1
                                << std::endl;
                            #endif

                            ctx.isend(msg_header1[b], 2, target1, tag);
                            ctx.isend(buffer_ptr[b], size1, target1, tag);
                        }

                        // message to second
                        {
                            msg_header2[b][0] = glob_first2;
                            msg_header2[b][1] = size2;

                            ctx.isend(msg_header2[b], 2, target2, tag);
                            ctx.isend(
                                buffer_ptr[b] + size1, size2, target2, tag);

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
                    }
                    assert(num_received == expect);
                }

                // synchronize after each level
                ctx.synchronize();
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
            hist->save(output + "." + WaveletMatrixBase::histogram_extension());
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
}
};

int main(int argc, char** argv) {
    return mpi_launch<mpi_wm_concat>(argc, argv);
}
