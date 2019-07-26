#pragma once

#include <distwt/thrill/wt.hpp>
#include <distwt/thrill/text.hpp>

#include <thrill/api/context.hpp>

#include <distwt/thrill/not_yet_templated.hpp>

class WaveletTreeLevelwise : public WaveletTree {
public:
    inline WaveletTreeLevelwise(
        const HistogramBase<sym_t>& hist,
        ctor_t construction_algorithm)
        : WaveletTree(hist, construction_algorithm) {
    }

    WaveletTreeLevelwise(
        const HistogramBase<sym_t>& hist,
        thrill::Context& ctx,
        const std::string& filename);

    void save(const std::string& filename);

    rawtext_t decode(thrill::Context& ctx, const HistogramBase<sym_t>& hist);
};
