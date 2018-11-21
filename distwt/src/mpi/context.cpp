#include <iomanip>
#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>

util::devnull MPIContext::m_devnull;

MPIContext::MPIContext(int* argc, char*** argv)
    : m_local_traffic({0,0,0,0,0,0}) {

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
    MPI_Finalize();
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

MPIContext::Traffic MPIContext::gather_traffic_data() const {
    Traffic glob;
    MPI_Allreduce(&m_local_traffic, &glob, Traffic::num_fields,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    return glob;
}
