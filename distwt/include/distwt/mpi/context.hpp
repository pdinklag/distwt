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

    inline int num_workers() { return m_num_workers; }
    inline int rank() { return m_rank; }

    std::ostream& cout();
    std::ostream& cout(bool b);
};
