#pragma once

#include <iostream>
#include <mpi.h>
#include <distwt/common/devnull.hpp>

class MPIContext {
private:
    static util::devnull m_devnull;

    int m_num_workers, m_rank;
    double m_start_time;

public:
    MPIContext(int* argc, char*** argv);
    ~MPIContext();

    inline size_t num_workers() const { return (size_t)m_num_workers; }
    inline size_t rank() const { return (size_t)m_rank; }

    std::ostream& cout() const;
    std::ostream& cout(bool b) const;
};
