#pragma once

#include <mpi.h>

class MPIContext {
private:
    int m_num_workers, m_rank;

public:
    MPIContext(int* argc, char*** argv);
    ~MPIContext();

    inline int num_workers() { return m_num_workers; }
    inline int rank() { return m_rank; }
};
