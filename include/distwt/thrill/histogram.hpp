#pragma once

#include <distwt/common/histogram.hpp>
#include <distwt/thrill/text.hpp>

class Histogram : public HistogramBase {
public:
    using HistogramBase::HistogramBase;

    Histogram(const rawtext_t& rawtext); // compute from text

    inline Histogram(const std::string& filename) { // load from file
        load(filename);
    }
};
