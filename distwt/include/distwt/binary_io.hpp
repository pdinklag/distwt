#pragma once

#include <iostream>

template<typename T>
void write_binary(std::ostream& out, const T& value) {
    out.write((const char*)&value, sizeof(T));
}

template<typename T>
T read_binary(std::istream& in) {
    T value;
    in.read((char*)&value, sizeof(T));
    return value;
}
