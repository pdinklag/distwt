#include <cassert>
#include <iomanip>
#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/malloc.hpp>

util::devnull MPIContext::m_devnull;
MPIContext* MPIContext::m_current = nullptr;

void MPIContext::on_alloc(size_t size) {
    if(m_current) m_current->track_alloc(size);
}

void MPIContext::on_free(size_t size) {
    if(m_current) m_current->track_free(size);
}

MPIContext::MPIContext(int* argc, char*** argv)
    : m_alloc_current(0),
      m_alloc_max(0),
      m_local_traffic({0,0,0,0,0,0}) {

    assert(!m_current);
    {
        m_current = this;
        malloc_callback::on_alloc = MPIContext::on_alloc;
        malloc_callback::on_free = MPIContext::on_free;
    }

    MPI_Init(argc, argv);
    int inum_workers, irank;

    MPI_Comm_size(MPI_COMM_WORLD, &inum_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &irank);

    m_rank = (size_t)irank;
    m_num_workers = (size_t)inum_workers;

    // determine workers per node via shared memory group size
    // we expect that this is the same on each node
    {
        MPI_Comm shmcomm;
        MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                            MPI_INFO_NULL, &shmcomm);

        int shmsize;
        MPI_Comm_size(shmcomm, &shmsize);

        m_workers_per_node = (size_t)shmsize;
    }

    // initial synchronization
    MPI_Barrier(MPI_COMM_WORLD);
    m_start_time = util::time();

    cout_master() << "MPIContext initialized with "
        << num_workers() << " workers on "
        << num_nodes() << " nodes ..."
        << std::endl;
}

MPIContext::~MPIContext() {
    if(m_current == this) {
        MPI_Finalize();

        malloc_callback::on_alloc = nullptr;
        malloc_callback::on_free = nullptr;
        m_current = nullptr;
    }
}

void MPIContext::count_traffic_tx(size_t target, size_t bytes) {
    if(same_node_as(target)) {
        m_local_traffic.tx_shm += bytes;
    } else {
        m_local_traffic.tx += bytes;
    }
}

void MPIContext::count_traffic_rx(size_t source, size_t bytes) {
    if(same_node_as(source)) {
        m_local_traffic.rx_shm += bytes;
    } else {
        m_local_traffic.rx += bytes;
    }
}

void MPIContext::track_alloc(size_t size) {
    m_alloc_current += size;
    m_alloc_max = std::max(m_alloc_max, m_alloc_current);
}

void MPIContext::track_free(size_t size) {
    assert(m_alloc_current >= size);
    m_alloc_current -= size;
}

std::ostream& MPIContext::cout() const {
    const double local_dt = util::time() - m_start_time;
    return (std::cout <<
        "[#" << m_rank <<
        " @" << std::setprecision(3) << std::fixed << local_dt << "] ");
}

std::ostream& MPIContext::cout(bool b) const {
    return b ? cout() : m_devnull;
}

void MPIContext::synchronize() {
    MPI_Barrier(MPI_COMM_WORLD);
}

size_t MPIContext::gather_max_alloc() const {
    size_t glob;
    MPI_Allreduce(&m_alloc_max, &glob, 1,
        MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);

    return glob;
}

MPIContext::Traffic MPIContext::gather_traffic() const {
    Traffic glob;
    MPI_Allreduce(&m_local_traffic, &glob, Traffic::num_fields,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    return glob;
}
