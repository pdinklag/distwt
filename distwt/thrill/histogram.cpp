#include <numeric>

#include <thrill/api/size.hpp>

#include <distwt/thrill/histogram.hpp>

Histogram::Histogram(thrill::Context& ctx, const rawtext_t& rawtext) {
    std::vector<size_t> hist(256);

    rawtext.Map([&](const symbol_t& c){ hist[c]++; return c; }).Size();
    hist = ctx.net.AllReduce(hist,
        thrill::common::ComponentSum<std::vector<size_t>>());

    for(size_t i = 0; i < 256; i++) {
        if(hist[i] > 0) {
            m_entries.emplace_back((symbol_t)i, hist[i]);
        }
    }
}
