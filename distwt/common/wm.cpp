#include <distwt/common/wm.hpp>

WaveletMatrixBase::WaveletMatrixBase(const HistogramBase& hist)
    : WaveletTreeBase(hist) {

    // allocate Z values
    m_z = std::vector<size_t>(height());
}
