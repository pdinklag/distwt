#include <array>
#include <tlx/cmdline_parser.hpp>

#include <distwt/common/util.hpp>
#include <distwt/mpi/context.hpp>
#include <distwt/mpi/file_partition_reader.hpp>

constexpr size_t SIGMA_MAX = 256ULL;

constexpr int TAG_LOCAL_HIST_ENTRY = 100;
constexpr int TAG_GLOBAL_HIST_ENTRY = 101;

int main(int argc, char** argv) {
    // Init MPI
    MPIContext ctx(&argc, &argv);

    // Read command-line
    tlx::CmdlineParser cp;

    size_t memory = 268'435'456ULL; // default to 256 MiB
    cp.add_bytes('m', "mem", memory, "Amount of available local memory.");

    std::string input_filename; // reuquired
    cp.add_param_string("file", input_filename, "The input file.");
    if (!cp.process(argc, argv)) {
        return -1;
    }

    // Compute local histogram
    FilePartitionReader input(ctx, input_filename);

    std::array<size_t, SIGMA_MAX> local_hist = {0};
    input.process_local([&](unsigned char c){
        ++local_hist[c];
    }, memory);

    // Define histogram entry message structure
    struct hist_entry_msg {
        unsigned char sym;
        size_t count;
    };

    MPI_Datatype hist_entry_msg_type;
    {
        int len[] =           {1, 1};
        MPI_Datatype type[] = {MPI_CHAR, MPI_LONG_LONG};
        MPI_Aint disp[] =     {0, 8};

        MPI_Type_create_struct(2, len, disp, type, &hist_entry_msg_type);
        MPI_Type_commit(&hist_entry_msg_type);
    }

    // Create outbox to responsible workers
    const size_t num_responsible = std::min(SIGMA_MAX, ctx.num_workers());
    {
        std::vector<std::vector<hist_entry_msg>> outbox(num_responsible);

        for(size_t c = 0; c < SIGMA_MAX; c++) {
            // find worker responsible for this symbol
            const size_t i = (c % ctx.num_workers());

            if(i == ctx.rank()) continue; // don't send to self

            // don't send zero entries
            if(local_hist[c] > 0) {
                outbox[i].push_back(
                    hist_entry_msg{(unsigned char)c, local_hist[c]});
            }
        }

        // Process outbox
        for(size_t i = 0; i < outbox.size(); i++) {
            if(i == ctx.rank()) continue; // don't send to self

            MPI_Send(outbox[i].data(), outbox[i].size() /* may be zero! */,
                hist_entry_msg_type, i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD);
        }
    }

    // Receive entries from other workers and update local histogram
    {
        hist_entry_msg recv_buffer[SIGMA_MAX];

        for(size_t i = 0; i < ctx.num_workers(); i++) {
            if(i == ctx.rank()) continue; // don't receive from self

            MPI_Status s;
            MPI_Probe(i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            int count;
            MPI_Get_count(&s, hist_entry_msg_type, &count);
            MPI_Recv(recv_buffer, count, hist_entry_msg_type,
                i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            for(size_t k = 0; k < size_t(count); k++) {
                local_hist[recv_buffer[k].sym] += recv_buffer[k].count;
            }
        }
    }

    // local histogram now contains global counts for those symbol that
    // this machine is responsible for
    // for those symbols, send out entries to all other machines
    {
        std::vector<hist_entry_msg> send;

        // gather
        for(size_t c = 0; c < SIGMA_MAX; c++) {
            // find worker responsible for this symbol
            const size_t responsible = (c % ctx.num_workers());

            if(responsible != ctx.rank()) continue; // only handle if responsible

            send.push_back(hist_entry_msg{(unsigned char)c, local_hist[c]});
        }

        // send to all other workers
        for(size_t i = 0; i < ctx.num_workers(); i++) {
            if(i == ctx.rank()) continue; // don't send to self

            MPI_Send(send.data(), send.size(),
                hist_entry_msg_type, i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD);
        }
    }

    // receive global histogram entries from responsible workers
    {
        hist_entry_msg recv_buffer[SIGMA_MAX];

        for(size_t c = 0; c < num_responsible; c++) {
            const size_t i = (c % ctx.num_workers());
            if(i == ctx.rank()) continue; // don't receive from self

            MPI_Status s;
            MPI_Probe(i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            int count;
            MPI_Get_count(&s, hist_entry_msg_type, &count);
            MPI_Recv(recv_buffer, count, hist_entry_msg_type,
                i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            for(size_t k = 0; k < size_t(count); k++) {
                local_hist[recv_buffer[k].sym] = recv_buffer[k].count;
            }
        }
    }

    // local_hist now contains the global histogram! :-)
    // debug
    for(size_t c = 0; c < SIGMA_MAX; c++) {
        if(local_hist[c] > 0) {
            ctx.cout() << "'" << (unsigned char)c << "' -> " << local_hist[c] << std::endl;
        }
    }

    return 0;
}
