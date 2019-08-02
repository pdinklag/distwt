#include <distwt/thrill/wm.hpp>

#include <thrill/api/write_binary.hpp>

void WaveletMatrix::save(const std::string& filename) {
    for(size_t l = 0; l < height(); l++) {
        m_bits[l].WriteBinary(filename + "." + level_extension(l));
    }
}