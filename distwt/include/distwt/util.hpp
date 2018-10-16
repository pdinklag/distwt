#pragma once

#include <fstream>
#include <vector>

namespace util {

inline size_t file_size(const std::string& filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

}

// stream output for bit vectors
inline std::ostream& operator<<(
    std::ostream& os, const std::vector<bool>& bv) {

    for(bool b : bv) {
        os << b ? '1' : '0';
    }

    return os;
}
