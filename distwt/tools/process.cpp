#include <bitset>
#include <fstream>
#include <iostream>

#include <tlx/cmdline_parser.hpp>

int main(int argc, const char** argv) {
    // Read command-line
    tlx::CmdlineParser cp;
    cp.set_description("Text processor utility");
    cp.set_author("Patrick Dinklage <patrick.dinklage@udo.edu>");

    std::string infile; // required
    cp.add_param_string("infile", infile, "The input file.");

    std::string outfile; // required
    cp.add_param_string("outfile", outfile, "The output file.");

    std::string filter;
    cp.add_string('f', "filter", filter, "The output will only contain the "
                                         "specified characters");

    size_t bufsize = 16ULL * 1024ULL * 1024ULL; // 16Mi
    cp.add_bytes('b', "buffer", bufsize, "The read/write buffer size");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    std::bitset<256> filter_keep;
    const bool filter_active = (filter.length() > 0);
    if(filter_active) {
        for(auto x : filter) {
            filter_keep[x] = 1;
        }
    }

    size_t read = 0, written = 0;
    {
        std::vector<char> wbuf(bufsize);
        size_t wp = 0;
        std::ofstream out(outfile);

        {
            std::vector<char> rbuf(bufsize);
            std::ifstream in(infile, std::ios::binary);

            bool in_eof = false;
            while(!in_eof) {
                in.read(rbuf.data(), bufsize);

                const size_t num = in.gcount();
                in_eof = in.eof();

                read += num;
                for(size_t i = 0; i < num; i++) {
                    // get next byte
                    const unsigned char c = rbuf[i];

                    // decide what to do with it
                    if(filter_active && !filter_keep[c]) {
                        continue;
                    }

                    // write
                    wbuf[wp++] = c;
                    if(wp >= bufsize) {
                        out.write(wbuf.data(), bufsize);
                        wp = 0;
                        written += bufsize;
                    }
                }
            }
        }

        // write remaining bytes
        if(wp > 0) {
            out.write(wbuf.data(), wp);
            written += wp;
        }
    }

    // status
    std::cout << read << " bytes read, "
        << written << " bytes written" << std::endl;

    return 0;
}
