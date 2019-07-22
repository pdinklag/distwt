#include <distwt/mpi/wt_nodebased.hpp>

//#define DBG_MERGE 1

WaveletTreeLevelwise WaveletTreeNodebased::merge(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const HistogramBase& hist,
    bool discard) {

    return WaveletTreeLevelwise(hist, // TODO: avoid recomputations!
        [&](WaveletTree::bits_t& bits, const WaveletTreeBase& wt){
            merge_impl(ctx, bits, wt, input, hist, discard, false);
        });
}

// TODO: compute Z values!
WaveletMatrix WaveletTreeNodebased::merge_to_matrix(
    MPIContext& ctx,
    const FilePartitionReader& input,
    const HistogramBase& hist,
    bool discard) {

    return WaveletMatrix(hist, // TODO: avoid recomputations!
        [&](WaveletMatrix::bits_t& bits, WaveletMatrix::z_t& z, const WaveletMatrixBase& wm){
            merge_impl(ctx, bits, wm, input, hist, discard, true);

            // compute Z values from histogram
            {
                const size_t sigma = hist.size();
                
                std::vector<size_t> sz(hist.size());
                for(size_t i = 0; i < sigma; i++) {
                    sz[i] = hist.entries[i].second;
                }

                // bottom-up, level by level
                for(size_t level = height() - 1; level > 0; level--) {
                    const size_t num_level_nodes = 1ULL << level;
                    size_t num0 = 0;

                    // compute Z
                    for(size_t i = 0; i < num_level_nodes; i++) {
                        num0 += sz[2 * i];
                    }
                    z[level] = num0;

                    // contract sizes
                    for(size_t i = 0; i < num_level_nodes; i++) {
                        sz[i] = sz[2 * i] + sz[2 * i] + 1;
                    }
                }

                // top level
                z[0] = sz[0];
            }
        });
}
