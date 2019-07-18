#include <distwt/mpi/effective_alphabet.hpp>

void EffectiveAlphabet::transform(
    const FilePartitionReader& input,
    std::function<void(symbol_t)> processor,
    const size_t rdbufsize) const {

    // compute effective transformation and call processor for each symbol
    input.process_local([&](symbol_t c){
        processor(m_map.at(c));
    }, rdbufsize);
}
