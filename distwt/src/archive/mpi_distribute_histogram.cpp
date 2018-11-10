/*
    I implemented this before I knew about MPI_Allreduce. It is practically the
    exact same thing and even has exactly the same running time.

    It is no longer needed, but I'm keeping this in the thesis repo anyway.
*/

// constexpr size_t SIGMA_MAX = 256ULL;

void distribute_histogram_group(
    const MPIContext& ctx,
    size_t* hist) {

    // define message structure
    struct entry_msg_t {
        unsigned char sym; // the symbol in question
        size_t count;      // the count of occurences in the text
    };

    // define MPI datatype
    MPI_Datatype entry_msg_mpitype;

    int len[] =           {1, 1};
    MPI_Datatype type[] = {MPI_CHAR, MPI_LONG_LONG};
    MPI_Aint disp[] =     {0, 8};

    MPI_Type_create_struct(2, len, disp, type, &entry_msg_mpitype);
    MPI_Type_commit(&entry_msg_mpitype);

    // Create outbox to responsible workers
    const size_t num_responsible = std::min(SIGMA_MAX, ctx.num_workers());
    {
        std::vector<std::vector<entry_msg_t>> outbox(num_responsible);

        for(size_t c = 0; c < SIGMA_MAX; c++) {
            // find worker responsible for this symbol
            const size_t i = (c % ctx.num_workers());

            if(i == ctx.rank()) continue; // don't send to self

            // don't send zero entries
            if(hist[c] > 0) {
                outbox[i].push_back(
                    entry_msg_t{(unsigned char)c, hist[c]});
            }
        }

        // Process outbox
        for(size_t i = 0; i < outbox.size(); i++) {
            if(i == ctx.rank()) continue; // don't send to self

            MPI_Send(outbox[i].data(), outbox[i].size() /* may be zero! */,
                entry_msg_mpitype, i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD);
        }
    }

    // Receive entries from other workers and update local histogram
    {
        entry_msg_t recv_buffer[SIGMA_MAX];

        for(size_t i = 0; i < ctx.num_workers(); i++) {
            if(i == ctx.rank()) continue; // don't receive from self

            MPI_Status s;
            MPI_Probe(i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            int count;
            MPI_Get_count(&s, entry_msg_mpitype, &count);
            MPI_Recv(recv_buffer, count, entry_msg_mpitype,
                i, TAG_LOCAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            for(size_t k = 0; k < size_t(count); k++) {
                hist[recv_buffer[k].sym] += recv_buffer[k].count;
            }
        }
    }

    // local histogram now contains global counts for those symbol that
    // this machine is responsible for
    // for those symbols, send out entries to all other machines
    {
        std::vector<entry_msg_t> send;

        // gather
        for(size_t c = 0; c < SIGMA_MAX; c++) {
            // find worker responsible for this symbol
            const size_t responsible = (c % ctx.num_workers());

            if(responsible != ctx.rank()) continue; // only handle if responsible

            send.push_back(entry_msg_t{(unsigned char)c, hist[c]});
        }

        // send to all other workers
        for(size_t i = 0; i < ctx.num_workers(); i++) {
            if(i == ctx.rank()) continue; // don't send to self

            MPI_Send(send.data(), send.size(),
                entry_msg_mpitype, i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD);
        }
    }

    // receive global histogram entries from responsible workers
    {
        entry_msg_t recv_buffer[SIGMA_MAX];

        for(size_t c = 0; c < num_responsible; c++) {
            const size_t i = (c % ctx.num_workers());
            if(i == ctx.rank()) continue; // don't receive from self

            MPI_Status s;
            MPI_Probe(i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            int count;
            MPI_Get_count(&s, entry_msg_mpitype, &count);
            MPI_Recv(recv_buffer, count, entry_msg_mpitype,
                i, TAG_GLOBAL_HIST_ENTRY, MPI_COMM_WORLD, &s);

            for(size_t k = 0; k < size_t(count); k++) {
                hist[recv_buffer[k].sym] = recv_buffer[k].count;
            }
        }
    }
}
