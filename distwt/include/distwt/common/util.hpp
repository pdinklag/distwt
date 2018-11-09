#pragma once

#include <fstream>
#include <vector>

#include <sys/stat.h>

namespace util {

inline size_t file_size(const std::string& filename) {
    struct stat buf;
    stat(filename.c_str(), &buf);
    return buf.st_size;
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
