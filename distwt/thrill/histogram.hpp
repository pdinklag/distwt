#pragma once

#include <distwt/common/histogram.hpp>
#include <distwt/thrill/text.hpp>

#include <thrill/api/context.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

class Histogram : public HistogramBase<sym_t, idx_t> {
public:
    using HistogramBase<sym_t, idx_t>::HistogramBase;

    Histogram(
        thrill::Context& ctx,
        const rawtext_t& rawtext); // compute from text

    inline Histogram(const std::string& filename) { // load from file
        load(filename);
    }
};
