#include "mpi_launcher.hpp"

#include <cassert>
#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <distwt/mpi/file_partition_reader.hpp>

#include <distwt/common/wt.hpp>
#include <distwt/mpi/histogram.hpp>
#include <distwt/mpi/effective_alphabet.hpp>
#include <distwt/mpi/wt_levelwise.hpp>

#include <distwt/mpi/result.hpp>

//#define DBG_BSORT 1

class mpi_bsort {
public:

template<typename sym_t>
static void start(
    MPIContext& ctx,
    const std::string& input_filename,
    const size_t prefix,
    const size_t in_rdbufsize,
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
    input.buffer(rdbufsize);
    
    time.input = dt();

    // Compute histogram
    ctx.cout_master() << "Compute histogram ..." << std::endl;
    Histogram<sym_t> hist(ctx, input, rdbufsize);

    time.hist = dt();

    // Compute effective alphabet
    EffectiveAlphabet<sym_t> ea(hist);

    // Transform text and cache in RAM
    ctx.cout_master() << "Compute effective transformation ..." << std::endl;
    std::vector<sym_t> etext(local_num);
    {
        size_t i = 0;
        ea.transform(input, [&](sym_t x){ etext[i++] = x; }, rdbufsize);
    }

    input.free();
    time.eff = dt();

    // Convert to level-wise representation
    auto wt = WaveletTreeLevelwise(hist,
    [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){

        const size_t height = wt.height();
        auto node_sizes = WaveletTreeBase::node_sizes(hist);

        bits.resize(height);

        #if DBG_BSORT
        ctx.cout() << "size per worker: " << input.size_per_worker()
                   << ", local_num: " << local_num
                   << std::endl;
        ctx.synchronize();
        #endif

        std::vector<sym_t> buffer(local_num);
        std::vector<idx_t> bucket_sizes;
        auto& bucket_pos = bucket_sizes; // alternative name to keep code readable

        std::vector<uint64_t*> msg_headers;
        for(size_t level = 0; level < height; level++) {
            const int tag = int(level);
            ctx.cout_master() << "level " << (level+1) << " ..." << std::endl;

            const size_t num_nlevel_nodes = 1ULL << (level+1);
            const size_t first_nlevel_node = num_nlevel_nodes;

            if(level+1 == height) {
                // free unneeded memory on last level
                buffer.clear();
                buffer.shrink_to_fit();
                
                bucket_sizes.shrink_to_fit();
                msg_headers.shrink_to_fit();
                
                node_sizes.clear();
                node_sizes.shrink_to_fit();
            }

            // construct bit vector
            auto& level_bits = bits[level];
            level_bits.resize(local_num);

            const size_t rsh = height - 1 - level;
            if(level+1 == height) {
                // this is the last level, build only the bit vector
                for(size_t i = 0; i < local_num; i++) {
                    level_bits[i] = bool((etext[i] >> rsh) & 1);
                }
            } else { // if level+1 < height
                // build bit and also fill the sort buckets
                bucket_sizes.resize(num_nlevel_nodes + 1); // one extra helper entry

                // scan 1 - precompute bucket sizes
                for(size_t i = 0; i < local_num; i++) {
                    const sym_t x = etext[i];
                    const size_t v = x >> rsh;
                    assert(v < num_nlevel_nodes);
                    ++bucket_sizes[v];
                }

                // compute bucket size (exclusive) prefix sums
                std::vector<idx_t> bucket_offs = bucket_sizes;
                ctx.ex_scan(bucket_offs);

                // locally exclusively prefix sum sizes to act as write positions
                {
                    idx_t local_offs = 0;
                    for(size_t v = 0; v < num_nlevel_nodes; v++) {
                        const idx_t sz = bucket_pos[v];
                        bucket_pos[v] = local_offs;
                        local_offs += sz;
                    }

                    assert(local_offs == idx_t(local_num));
                    bucket_pos[num_nlevel_nodes] = local_num; // helper entry
                }
                
                // scan 2 - fill buckets and write bit vector
                for(size_t i = 0; i < local_num; i++) {
                    const sym_t x = etext[i];
                    const size_t v = x >> rsh;
                    level_bits[i] = bool(v & 1);
                    buffer[bucket_pos[v]] = x;
                    ++bucket_pos[v];
                }

                assert(bucket_pos[num_nlevel_nodes-1] == idx_t(local_num));

                // revert bucket pos writes by shifting
                for(size_t v = num_nlevel_nodes - 1; v > 0; v--) {
                    bucket_pos[v] = bucket_pos[v-1];
                }
                bucket_pos[0] = 0;

                // distribute buckets
                // -> using locality to apply merge directly unlike after DD!
                // -> this corresponds to bucket sort with < sigma keys

                // send buckets away
                size_t glob_node_offs = 0;
                for(size_t v = 0; v < num_nlevel_nodes; v++) {
                  const size_t bsz = bucket_pos[v+1] - bucket_pos[v];
                  if(bsz > 0) {

                    const size_t glob_bucket_offs =
                        glob_node_offs + bucket_offs[v];

                    #ifdef DBG_BSORT
                    ctx.cout() << "processing bucket " << v
                        << " with global offset = " << glob_bucket_offs
                        << " (glob_node_offs = " << glob_node_offs
                        << ", prefix sum = " << bucket_offs[v] << ")"
                        << std::endl;
                    #endif

                    // target of first bucket item
                    const size_t target1 =
                        glob_bucket_offs / input.size_per_worker();

                    // target of last bucket item
                    const size_t glob_last =
                        glob_bucket_offs + bsz - 1;
                    const size_t target2 =
                        glob_last / input.size_per_worker();

                    #ifdef DBG_BSORT
                    ctx.cout() << "substring [" << glob_bucket_offs
                        << ", " << glob_last << "] for bucket " << v
                        << " goes to target1=" << target1
                        << " and target2=" << target2
                        << std::endl;
                    #endif

                    if(target1 == target2) {
                        // whole bucket goes to target
                        const size_t target = target1;

                        // send one message
                        auto header = new uint64_t[2];
                        header[0] = glob_bucket_offs;
                        header[1] = bsz;
                        msg_headers.push_back(header);

                        ctx.isend(header, 2, target, tag);
                        ctx.isend(buffer.data() + bucket_pos[v], bsz, target, tag);

                        #ifdef DBG_BSORT
                        ctx.cout()
                            << "(single) send " << bsz << " at "
                            << glob_bucket_offs << " (local 0)"
                            << " to " << target
                            << std::endl;
                        #endif
                    } else {
                        // bucket can go to at most two different targets!
                        assert(target1 + 1 == target2);

                        const size_t glob_first2 =
                            target2 * input.size_per_worker();
                        assert(glob_first2 > glob_bucket_offs);
                        assert(glob_first2 <= glob_last);

                        const size_t size1 = glob_first2 - glob_bucket_offs;
                        const size_t size2 = glob_last - glob_first2 + 1;

                        // message to first
                        {
                            auto header1 = new uint64_t[2];
                            header1[0] = glob_bucket_offs;
                            header1[1] = size1;
                            msg_headers.push_back(header1);

                            #ifdef DBG_BSORT
                            ctx.cout()
                                << "(#1) send " << size1 << " at "
                                << glob_bucket_offs << " (local 0)"
                                << " to " << target1
                                << std::endl;
                            #endif

                            ctx.isend(header1, 2, target1, tag);
                            ctx.isend(buffer.data() + bucket_pos[v], size1, target1, tag);
                        }

                        // message to second
                        {
                            auto header2 = new uint64_t[2];
                            header2[0] = glob_first2;
                            header2[1] = size2;
                            msg_headers.push_back(header2);

                            ctx.isend(header2, 2, target2, tag);
                            ctx.isend(buffer.data() + bucket_pos[v] + size1, size2, target2, tag);

                            #ifdef DBG_BSORT
                            ctx.cout()
                                << "(#2) send " << size2 << " at "
                                << glob_first2 << " (local " << size1
                                << ") to " << target2
                                << std::endl;
                            #endif
                        }
                    }
                  } else {
                        #ifdef DBG_BSORT
                        ctx.cout() << "skipping bucket " << v
                            << " because it is empty"
                            << std::endl;
                        #endif
                  }
                  glob_node_offs += node_sizes[first_nlevel_node-1+v];
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

                        #ifdef DBG_BSORT
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

                        #ifdef DBG_BSORT
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
                bucket_sizes.clear();

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
            hist.save(output + "." + WaveletTreeBase::histogram_extension());
        }

        wt.save(ctx, output);
    }

    // Synchronize for exit
    ctx.cout_master() << "Waiting for exit signals ..." << std::endl;
    ctx.synchronize();

    // gather stats
    Result result("mpi-bsort", ctx, input, wt.sigma(), time);

    ctx.cout_master() << result.readable() << std::endl
                      << result.sqlplot() << std::endl;
}
};

int main(int argc, char** argv) {
    return mpi_launch<mpi_bsort>(argc, argv);
}
