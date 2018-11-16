#include <iomanip>
#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>

util::devnull MPIContext::m_devnull;

MPIContext::MPIContext(int* argc, char*** argv) {
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
    MPI_Status status;
    if(is_master()) {
        // on master, wait for ready message from all other nodes
        for(size_t i = 1; i < m_num_workers; i++) {
            MPI_Recv(nullptr, 0, MPI_BYTE, (int)i, SYNC_RDY_TAG, MPI_COMM_WORLD, &status);
        }

        // once all ready messages are there, dispatch ack messages
        for(size_t i = 1; i < m_num_workers; i++) {
            MPI_Send(nullptr, 0, MPI_BYTE, (int)i, SYNC_ACK_TAG, MPI_COMM_WORLD);
        }
    } else {
        // on all others, first send ready message to master
        MPI_Send(nullptr, 0, MPI_BYTE, 0, SYNC_RDY_TAG, MPI_COMM_WORLD);

        // then wait for ack from master
        MPI_Recv(nullptr, 0, MPI_BYTE, 0, SYNC_ACK_TAG, MPI_COMM_WORLD, &status);
    }
}

void MPIContext::exit() {
    if(is_master()) {
        // on master, wait for EXIT_TAG message from all other nodes
        MPI_Status status;
        for(size_t i = 1; i < m_num_workers; i++) {
            MPI_Recv(nullptr, 0, MPI_BYTE, (int)i, EXIT_TAG, MPI_COMM_WORLD, &status);
        }
    } else {
        // on all others, send EXIT_TAG message to master
        MPI_Send(nullptr, 0, MPI_BYTE, 0, EXIT_TAG, MPI_COMM_WORLD);
    }
}
