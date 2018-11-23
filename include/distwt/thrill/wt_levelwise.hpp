#pragma once

#include <distwt/thrill/wt.hpp>
#include <distwt/thrill/text.hpp>

#include <thrill/api/context.hpp>

class WaveletTreeLevelwise : public WaveletTree {
public:
    inline WaveletTreeLevelwise(
        const HistogramBase& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    WaveletTreeLevelwise(
        const HistogramBase& hist,
        thrill::Context& ctx,
        const std::string& filename);

    void save(const std::string& filename);

    rawtext_t decode(thrill::Context& ctx, const HistogramBase& hist);
};