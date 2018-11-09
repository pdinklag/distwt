#include <distwt/common/util.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

FilePartitionReader::FilePartitionReader(
    const MPIContext& ctx,
    const std::string& filename)
    : m_filename(filename) {

    m_total_size = util::file_size(m_filename);
    const size_t sz_per_worker = m_total_size / size_t(ctx.num_workers()); // TODO: CEIL!!

    m_local_offset = sz_per_worker * size_t(ctx.rank());
    const size_t local_end = std::min(
        m_local_offset + sz_per_worker, m_total_size);

    m_local_num = local_end - m_local_offset;
}
