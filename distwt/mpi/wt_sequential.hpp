#pragma once

#include <distwt/common/effective_alphabet.hpp>

#include <distwt/mpi/context.hpp>
#include <distwt/mpi/wt.hpp>

// prefix counting
void wt_pc(
    const WaveletTreeBase& wt,
    WaveletTree::bits_t& bits,
    const MPIContext& ctx,
    const std::vector<esym_t>& text);

// example algorithm from Navarro's book
void wt_navarro(
    WaveletTree::bits_t& bits,
    const MPIContext& ctx,
    const size_t node_id,
    esym_t* text,
    const size_t n,
    const size_t a,
    const size_t b,
    esym_t* buffer);
