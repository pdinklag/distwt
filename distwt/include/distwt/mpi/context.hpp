#pragma once

#include <iostream>
#include <vector>

#include <mpi.h>

#include <distwt/common/devnull.hpp>

#include <distwt/mpi/mpi_type.hpp>

class MPIContext {
public:
    struct ProbeResult {
        size_t size;
        size_t sender;
    };

    struct Traffic {
        size_t tx, rx, tx_est, rx_est;
    };

private:
    static constexpr int SYNC_RDY_TAG = 777;
    static constexpr int SYNC_ACK_TAG   = 778;

    static constexpr int EXIT_TAG = 666;

    static util::devnull m_devnull;

    int m_num_workers, m_rank;
    double m_start_time;

    Traffic m_local_traffic;

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

    Traffic gather_traffic_data() const;

    template<typename T>
    void send(const T *buf, size_t num, size_t target, int tag = 0) {
        MPI_Send(buf, num, mpi_type<T>::id(), target, tag, MPI_COMM_WORLD);
        m_local_traffic.tx += num * sizeof(T);
    }

    template<typename T>
    inline void send(const std::vector<T>& v, size_t target, int tag = 0) {
        send(v.data(), v.size(), target, tag);
    }

    template<typename T>
    MPI_Status recv(T* buf, size_t num, size_t source, int tag = 0) {
        MPI_Status st;
        MPI_Recv(buf, num, mpi_type<T>::id(), source, tag, MPI_COMM_WORLD, &st);
        m_local_traffic.rx += num * sizeof(T);
        return st;
    }

    template<typename T>
    inline MPI_Status recv(std::vector<T>& v, size_t num, size_t source, int tag = 0) {
        v.resize(num);
        v.shrink_to_fit();
        return recv(v.data(), num, source, tag);
    }

    template<typename T>
    MPI_Request isend(const T *buf, size_t num, size_t target, int tag = 0) {
        MPI_Request req;
        MPI_Isend(buf, num, mpi_type<T>::id(), target, tag, MPI_COMM_WORLD, &req);
        m_local_traffic.tx += num * sizeof(T);
        return req;
    }

    template<typename T>
    ProbeResult probe(size_t source = MPI_ANY_SOURCE, int tag = 0) {
        MPI_Status st;
        MPI_Probe(source, tag, MPI_COMM_WORLD, &st);

        int size;
        MPI_Get_count(&st, mpi_type<T>::id(), &size);

        return ProbeResult{ (size_t)size, (size_t)st.MPI_SOURCE };
    }

    template<typename T>
    inline ProbeResult probe(int tag) {
        return probe<T>(MPI_ANY_SOURCE, tag);
    }

    template<typename T>
    inline void all_reduce(const T* sbuf, T* rbuf, size_t num, MPI_Op op = MPI_SUM) {
        MPI_Allreduce(sbuf, rbuf, num, mpi_type<T>::id(), op, MPI_COMM_WORLD);

        // estimate TX/RX
        const size_t num_responsible = std::min(num, num_workers());

        size_t my_num = 0;
        for(size_t i = rank(); i < num; i += num_responsible) {
            ++my_num;
        }

        const size_t msg_size = sizeof(int) + sizeof(T); // index and value
        const size_t traffic =
            (num - my_num) * msg_size // send own data to responsible
            + my_num * (num_workers() - 1) * msg_size; // broadcast data I'm responsible for

        m_local_traffic.tx_est += traffic;
        m_local_traffic.rx_est += traffic;
    }

    void synchronize();
};

