#pragma once

#include <iostream>
#include <vector>

#include <tlx/math/integer_log2.hpp>

#include <mpi.h>

#include <distwt/common/devnull.hpp>
#include <distwt/common/util.hpp>

#include <distwt/mpi/mpi_type.hpp>

class MPIContext {
private:
    static util::devnull m_devnull;
    static MPIContext* m_current;

public:
    static inline MPIContext* current() { return m_current; }
    static void on_alloc(size_t size);
    static void on_free(size_t size);

public:
    struct ProbeResult {
        bool   avail;
        size_t size;
        size_t sender;
    };

    struct Traffic {
        static constexpr size_t num_fields = 6;
        size_t tx, rx, tx_est, rx_est, tx_shm, rx_shm;
    };

private:
    MPI_Comm m_comm;

    size_t m_num_workers, m_rank;
    size_t m_workers_per_node;
    double m_start_time;

    Traffic m_local_traffic;
    size_t m_alloc_current, m_alloc_max;

    void count_traffic_tx(size_t target, size_t bytes);
    void count_traffic_rx(size_t source, size_t bytes);

    void count_traffic_tx_est(size_t target, size_t bytes);
    void count_traffic_rx_est(size_t source, size_t bytes);

    void track_alloc(size_t size);
    void track_free(size_t size);

public:
    MPIContext(int* argc, char*** argv);
    ~MPIContext();

    inline double time() const { return MPI_Wtime(); }

    inline size_t num_workers() const { return m_num_workers; }
    inline size_t rank() const { return m_rank; }

    inline size_t num_nodes() const {
        return m_num_workers / m_workers_per_node;
    }

    inline size_t num_workers_per_node() const {
        return m_workers_per_node;
    }

    inline size_t node_rank(size_t worker) {
        return worker / m_workers_per_node;
    }

    inline size_t node_rank() {
        node_rank(m_rank);
    }

    inline bool same_node_as(size_t other) {
        return node_rank() == node_rank(other);
    }

    inline bool is_master() const { return m_rank == 0; }

    inline MPI_Comm comm() const { return m_comm; }
    void set_comm(MPI_Comm comm);

    std::ostream& cout() const;
    std::ostream& cout(bool b) const;

    inline std::ostream& cout_master() const {
        return cout(is_master());
    }

    inline size_t local_alloc_current() const { return m_alloc_current; }
    inline size_t local_alloc_max() const { return m_alloc_max; }

    Traffic gather_traffic() const;
    size_t gather_max_alloc() const;

    template<typename T>
    void send(const T *buf, size_t num, size_t target, int tag = 0) {
        MPI_Send(buf, num, mpi_type<T>::id(), target, tag, m_comm);
        count_traffic_tx(target, num * sizeof(T));
    }

    template<typename T>
    inline void send(const std::vector<T>& v, size_t target, int tag = 0) {
        send(v.data(), v.size(), target, tag);
    }

    template<typename T>
    MPI_Status recv(T* buf, size_t num, size_t source, int tag = 0) {
        MPI_Status st;
        MPI_Recv(buf, num, mpi_type<T>::id(), source, tag, m_comm, &st);
        count_traffic_rx(source, num * sizeof(T));
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
        MPI_Isend(buf, num, mpi_type<T>::id(), target, tag, m_comm, &req);
        count_traffic_tx(target, num * sizeof(T));
        return req;
    }

    template<typename T>
    inline void isend(const std::vector<T>& v, size_t target, int tag = 0) {
        isend(v.data(), v.size(), target, tag);
    }

    template<typename T>
    ProbeResult probe(size_t source = MPI_ANY_SOURCE, int tag = 0) {
        MPI_Status st;
        MPI_Probe(source, tag, m_comm, &st);

        int size;
        MPI_Get_count(&st, mpi_type<T>::id(), &size);

        return ProbeResult{ true, (size_t)size, (size_t)st.MPI_SOURCE };
    }

    template<typename T>
    inline ProbeResult probe(int tag) {
        return probe<T>(MPI_ANY_SOURCE, tag);
    }

private:
    inline void simulate_allreduce_traffic(const size_t msg_size) {
        // simulates the all reduce operation for traffic measurement
        // based on the merge tree algorithm
        const size_t logp = tlx::integer_log2_ceil(m_num_workers);
        for(size_t level = 0; level < logp; level++) {
            const size_t q = (1ULL << level);
            const size_t q_prev = (1ULL << (level-1));
            const size_t v = m_rank / q;

            if((v % 2) == 0ULL) {
                if(level+1 < logp) {
                    // reduction: I send a message to my right sibling
                    count_traffic_tx_est(m_rank + q, msg_size);

                    // broadcast: I receive a message from my right sibling
                    count_traffic_rx_est(m_rank + q, msg_size);
                }
            }

            if(level > 0) {
                // reduction: I receive a message from my left sibling
                //            of the previous level
                count_traffic_rx_est(m_rank - q_prev, msg_size);

                // broadcast: I send a message to my left sibling
                //            of the previous level
                count_traffic_tx_est(m_rank - q_prev, msg_size);
            }
        }
    }

public:
    template<typename T>
    inline void all_reduce(
        const T* sbuf, T* rbuf, size_t num, MPI_Op op = MPI_SUM) {

        MPI_Allreduce(sbuf, rbuf, num, mpi_type<T>::id(), op, m_comm);
        simulate_allreduce_traffic(sizeof(int) + num * sizeof(T));
    }

    template<typename T>
    inline void all_reduce(std::vector<T>& v, MPI_Op op = MPI_SUM) {
        std::vector<T> rbuf(v.size());
        all_reduce(v.data(), rbuf.data(), v.size(), op);
        v = rbuf;
    }

private:
    inline void simulate_scan_traffic(const size_t msg_size) {
        // simulates the scan operation for traffic measurement
        // based on the merge tree algorithm
        const size_t logp = tlx::integer_log2_ceil(m_num_workers);
        for(size_t level = 0; level < logp; level++) {
            const size_t q = (1ULL << level);
            const size_t q_prev = (1ULL << (level-1));
            const size_t v = m_rank / q;

            if((v % 2) == 0ULL) {
                if(level+1 < logp) {
                    // bottom-up: I send a message to my right sibling
                    count_traffic_tx_est(m_rank + q, msg_size);

                    if(m_rank > 0) {
                        // top-down: I receive a message from my left sibling
                        count_traffic_tx_est(m_rank - q, msg_size);
                    }
                }
            }

            if(level > 0) {
                // bottom-up: I receive a message from my left sibling
                //            of the previous level
                count_traffic_rx_est(m_rank - q_prev, msg_size);

                if((v % 2) == 0ULL) {
                    // top-down: I send a message to my right sibling of the
                    //           previous level
                    count_traffic_tx_est(m_rank + q_prev, msg_size);
                }
            }
        }
    }

public:
    template<typename T>
    inline void scan(std::vector<T>& v, MPI_Op op = MPI_SUM) {
        std::vector<T> rbuf(v.size());
        MPI_Scan(v.data(), rbuf.data(), v.size(),
            mpi_type<T>::id(), op, m_comm);
        v = rbuf;
        simulate_scan_traffic(sizeof(int) + v.size() * sizeof(T));
    }

    template<typename T>
    inline void ex_scan(std::vector<T>& v, MPI_Op op = MPI_SUM) {
        std::vector<T> rbuf(v.size());
        MPI_Exscan(v.data(), rbuf.data(), v.size(),
            mpi_type<T>::id(), op, m_comm);
        v = rbuf;
        simulate_scan_traffic(sizeof(int) + v.size() * sizeof(T));
    }

    void synchronize();
};

