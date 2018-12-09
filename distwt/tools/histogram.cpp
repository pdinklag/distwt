#include <array>
#include <fstream>
#include <iomanip>
#include <iostream>

#include <tlx/cmdline_parser.hpp>

constexpr size_t N = 256;

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;
    cp.set_description("Computes and prints the histogram of the given file");
    cp.set_author("Patrick Dinklage <patrick.dinklage@udo.edu>");

    std::string filename; // required
    cp.add_param_string("file", filename, "The input file.");

    size_t rbufsize = 4ULL * 1024ULL * 1024ULL; // 4Mi
    cp.add_bytes('r', "rbuf", rbufsize, "The read buffer size");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    // compute histogram
    size_t total = 0;
    std::array<size_t, N> hist{};
    {
        unsigned char rbuf[rbufsize];
        std::ifstream in(filename, std::ios::binary);

        bool eof = false;
        while(!eof) {
            in.read((char*)rbuf, rbufsize);
            
            const size_t num = in.gcount();
            eof = in.eof();

            total += num;
            for(size_t i = 0; i < num; i++) {
                ++hist[rbuf[i]];
            }
        }
    }

    // print histogram
    std::cout << "Read " << total << " bytes in total. Histogram:" << std::endl;
    for(size_t i = 0; i < N; i++) {
        if(hist[i] > 0) {
            std::cout << "   0x" << std::setw(2) << std::setfill('0')
                << std::uppercase << std::hex << i;

            switch(i) {
                case 0x9: std::cout << " ( \\t)"; break;
                case 0xA: std::cout << " ( \\n)"; break;
                case 0xD: std::cout << " ( \\r)"; break;
                default:  std::cout << " ('" << (char)i << "')"; break;
            }

            std::cout << ": " << std::dec << hist[i] << std::endl;
        }
    }

    return 0;
}
