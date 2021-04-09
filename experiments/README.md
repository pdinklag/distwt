# Experiments

This directory contains scripts for the reproduction of results given in the paper [*Constructing the Wavelet Tree and Wavelet Matrix in Distributed Memory*](https://doi.org/10.1137/1.9781611976007.17) [Dinklage et al., ALENEX 2020] as well as the journal article on *Practical Wavelet Tree Construction*.

## Requirements

In order to run the experiments, the MPI related binaries need to be built as described in the root Readme. The Thrill binaries are not required. The system to conduct the experiments requires MPI. Note that for the aforementioned literature, [Intel MPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi/components/mpi-library.html) 2018.3 was used to conduct the experiments.

### Configuration

The following system-specific configuration is required:

* At the beginning of `schedule.sh`, change the `WORK` and `BIN` variables as needed. Note that these must contain absolute paths in case you are using Slurm.
* Further down, you may also need to configure the `DATASETS` variable to contain the path to the input datasets. The input files and further properties for the experiments are determined right below.
* When using Slurm, in `mpi5.slurm`, you may need to adapt the `#SBATCH` configuration lines to your enviroment. Ensure that `--ntasks-per-node` contains exactly the number of cores per node in order to use them maximally, `--cpus-per-task` should remain at one. The `--nodes` setting is simply a template that will be overridden by the script described below.

## Running

The entry point for the experiments is the `schedule.sh` bash script that contains the same configuration as used in the aforementioned literature.

It expects one command-line argument:

* `weak` for the weak scaling experiments (1 GiB of input per node),
* `breakdown` for the breakdown experiment (fixed number of nodes with increasing input sizes) and
* `cost` for the COST experiment (fixed input size of 4 GiB for an increasing number of nodes).

When executed, the script prints the command lines for the corresponding experiments to the standard output. These are targeted at a [Slurm Workload Manager](https://slurm.schedmd.com/). If Slurm is not available in your environment, replace the `sbatch mpi5.slurm` portion of the command lines and use the remaining command lines as needed in your environment (e.g., prepend `mpirun`).

## Results

The applications print their results to the standard output, which Slurm typcially forwards to log files.

The raw experiment results are contained in those lines that begin with `RESULT`, followed by the raw data in a simple `key=value` format. In order to extract all (and only) the results from multiple log files, you can use `grep -h RESULT *.log`.

### Result Keys

The following table lists the keys that are contained in `RESULT` lines and their meaning.

| Key                | Description                                                  | Unit    |
| ------------------ | ------------------------------------------------------------ | ------- |
| `algo`             | Name (internal) of the executed wavelet tree or matrix construction algorithm. | &ndash; |
| `nodes`            | Number of nodes used for distributed computation.            | #       |
| `workers_per_node` | Number of processing elements used on each node, i.e., `p = nodes * workers_per_node` | #       |
| `input`            | Input text file.                                             | &ndash; |
| `size`             | The number of characters in the input.                       | B       |
| `bps`              | Number of bytes per character in the input file.             | B       |
| `alphabet`         | Size of the alphabet encountered in the input file.          | #       |
| `time_input`       | Time needed to read the local input parts on all nodes.      | s       |
| `time_hist`        | Time needed for the distributed histogram computation.       | s       |
| `time_eff`         | Time needed to determine the effective alphabet and to transform the local input parts. | s       |
| `time_construct`   | Time for *local* wavelet tree construction in case a merge step is required afterwards (e.g., domain decomposition). For algorithms that need no subsequent merge (e.g., sorting), this is already the entire construction time. | s       |
| `time_merge`       | Time require for merging local wavelet trees. May be zero if the algorithm does not need a merge. | s       |
| `memory`           | Cumulative amount of RAM used by all nodes.                  | B       |
| `traffic`          | Cumulative amount of data sent.                              | B       |

Note that the `time` values should be interpreted as actual wall clock times, *not* as work. Contrary to that, memory and traffic values are cumulative.

### Queries

For the publications, the raw data was entered into an SQL database (via [sqlplot-tools](https://github.com/bingmann/sqlplot-tools)) and queried as follows, grouping the result lines by the algorithm (`algo`) for computing medians or averages.

* *Throughputs* are computed by dividing the output size in bits (*n lg σ*) by the time needed for histogram computation, wavelet tree construction and merging, followed by a downscaling to *Gibit/s*:

  ```
  ((size*CEIL(LOG(alphabet)/LOG(2)))/MEDIAN(time_construct+time_merge+time_hist))/(1024.0*1024.0*1024.0)
  ```

* The *average memory per node* is computed simply by dividing the cumulative memory by the number of nodes, scaled down to *GiB*:

  ```
  (AVG(memory)/nodes)/(1024.0*1024.0*1024.0)
  ```

* *Average traffic* values are simply scaled down to *GiB*:

  ```
  AVG(traffic)/(1024.0*1024.0*1024.0)
  ```

