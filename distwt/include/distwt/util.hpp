#pragma once

#include <fstream>

namespace util {

inline size_t file_size(const std::string& filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

}
