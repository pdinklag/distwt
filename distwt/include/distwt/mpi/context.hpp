#pragma once

#include <iostream>
#include <mpi.h>
#include <distwt/common/devnull.hpp>

class MPIContext {
private:
    static constexpr int SYNC_RDY_TAG = 777;
    static constexpr int SYNC_ACK_TAG   = 778;

    static constexpr int EXIT_TAG = 666;

    static util::devnull m_devnull;

    int m_num_workers, m_rank;
    double m_start_time;

public:
    MPIContext(int* argc, char*** argv);
    ~MPIContext();

    inline size_t num_workers() const { return (size_t)m_num_workers; }
    inline size_t rank() const { return (size_t)m_rank; }

    inline bool is_master() const { return m_rank == 0; }

    std::ostream& cout() const;
    std::ostream& cout(bool b) const;

    inline std::ostream& cout_master() const {
        return cout(is_master());
    }

    void synchronize();
    void exit();
};
