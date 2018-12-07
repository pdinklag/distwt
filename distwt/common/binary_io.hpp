#pragma once

#include <fstream>
#include <iostream>

namespace binary {

class FileWriter {
private:
    std::ofstream m_out;

public:
    inline FileWriter(const std::string& filename)
        : m_out(filename, std::ios::binary) {
    }

    inline ~FileWriter() {
    }

    template<typename T>
    inline void write(const T& value) {
        m_out.write((const char*)&value, sizeof(T));
    }
};

class FileReader {
private:
    std::ifstream m_in;

public:
    inline FileReader(const std::string& filename)
        : m_in(filename, std::ios::binary) {
    }

    inline ~FileReader() {
    }

    template<typename T>
    inline T read() {
        T value;
        m_in.read((char*)&value, sizeof(T));
        return value;
    }
};

}
