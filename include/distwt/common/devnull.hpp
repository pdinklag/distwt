#pragma once

#include <iostream>

namespace util {
    class nullbuf : public std::streambuf {
    public:
        inline int overflow(int c) { return c; }
    };

    class devnull : public std::ostream {
    private:
        nullbuf m_buf;

    public:
        inline devnull() : std::ostream(&m_buf) {
        }
    };

}
