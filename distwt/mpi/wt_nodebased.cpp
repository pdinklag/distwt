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
            const size_t sigma = hist.size();
            size_t mask = 1ULL << (height() - 1);
            
            for(size_t level = 0; level < height(); level++) {
                size_t num0 = 0;
                for(size_t i = 0; i < sigma; i++) {
                    if((i & mask) == 0) num0 += hist.entries[i].second;
                }
                z[level] = num0;
                mask >>= 1ULL;
            }
        });
}
