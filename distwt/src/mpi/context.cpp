#include <iomanip>
#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>

util::devnull MPIContext::m_devnull;

MPIContext::MPIContext(int* argc, char*** argv)
    : m_local_traffic({0,0,0,0}) {
    MPI_Init(argc, argv);
    MPI_Comm_size(MPI_COMM_WORLD, &m_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &m_rank);
    m_start_time = util::time();
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
    MPI_Allreduce(&m_local_traffic, &glob, 4,
        MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

    return glob;
}
