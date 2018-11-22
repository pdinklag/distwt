#pragma once

#include <functional>
#include <distwt/common/effective_alphabet.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

class EffectiveAlphabet : public EffectiveAlphabetBase {
public:
    using EffectiveAlphabetBase::EffectiveAlphabetBase;

    void transform(
        const FilePartitionReader& input,
        std::function<void(symbol_t)> processor,
        const size_t rdbufsize) const;
};
