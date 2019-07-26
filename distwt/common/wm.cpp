#include <distwt/common/wm.hpp>
#include <distwt/common/binary_io.hpp>

void WaveletMatrixBase::save_z(const std::string& filename) const {
    binary::FileWriter w(filename);

    for(size_t level = 0; level < height(); level++) {
        w.write<size_t>(z(level)); // z value for level
    }
}
