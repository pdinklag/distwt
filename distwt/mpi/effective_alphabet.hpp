#pragma once

#include <functional>
#include <distwt/common/effective_alphabet.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

template<typename sym_t>
class EffectiveAlphabet : public EffectiveAlphabetBase<sym_t> {
public:
    using EffectiveAlphabetBase<sym_t>::EffectiveAlphabetBase;

    void transform(
        const FilePartitionReader<sym_t>& input,
        std::function<void(sym_t)> processor,
        const size_t rdbufsize) const {

        // compute effective transformation and call processor for each symbol
        input.process_local([&](sym_t c){
            processor(this->m_map.at(c));
        }, rdbufsize);
    }
};
