# Distributed Wavelet Tree and Wavelet Matrix Construction
This repository contains the code I wrote in the scope of my master's thesis on the distributed construction of wavelet trees (WT) and wavelet matrices (WM).

It is split up into two *worlds*: the Thrill world contains algorithms based on the [Thrill framework](http://project-thrill.org/), the MPI world is based on MPI. While Thrill allows for swift high-level sketching of algorithms which almost immediately works in practice, you will find a lot more low-level yada in the MPI part which may be useful for other projects, too (e.g. the parallel split operation).

## Building
The project is built like a typical CMake project:
```
mkdir build && cd build
cmake ..
make
```

### Dependencies
The only hard external dependency is MPI. The project also uses [tlx](https://github.com/tlx/tlx), which is embedded as a git submodule that is automatically initialized via CMake.

Thrill binaries will only be built if Thrill is available. Pass `-DTHRILL_ROOT_DIR=<path/to/thrill>` to CMake pointing to a fully built clone of the [Thrill repository](https://github.com/thrill/thrill).

## Algorithms and Helpers
Here's a quick roundup of the generated binaries.

### WT and WM Construction
By default, the data structures are constructed only in RAM (or semi-externally at Thrill's discretion). All binaries accept a `-o <file>` command line parameter to specify an output file, as well as the `-p <bytes>` parameter to tell them to process only that specified prefix of the input file. See below for details on the output.

#### MPI

Binary | Description
------ | -----------
`mpi-bsort` | WT construction using stable bucket sorting. Buckets are pre-allocated - for this, the current text has to be scanned once in advance. This causes a lower memory profile than `mpi-dynbsort` at the cost of the extra scan on each level.
`mpi-dd` | WT construction using domain decomposition.
`mpi-dsplit` | WT construction using the distributed split operation.
`mpi-dynbsort` | WT construction using stable bucket sorting. Buckets are filled on the fly using `std::vector`'s capacity doubling, causing some excess memory to be allocated, but saving the extra scan that `mpi-bsort` needs.
`mpi-wm-concat` | WM construction using bucket concatenation, i.e. bucket sorting with two buckets on each level.
`mpi-wm-dd` | WM construction using domain decomposition. Equivalent to the corresponding WT algorithm, just that the communication pattern is adapted to build the wavelet matrix instead.
`mpi-wm-dsplit` | WM construction using the distributed split operation. Equivalent to the corresponding WT algorithm, just that the communication pattern is adapted to build the wavelet matrix instead.

The `mpi` binaries accept the `-l <local_file>` parameter. If given, a worker's local part of the input file will be extracted to `local_file` in a preliminary step, so "chaotic" parallel access to the input, which may have heavy hits on the performance, can be avoided.

##### Thrill

Binary | Description
------ | -----------
`thrill-bsort` | WT construction using stable bucket sorting on each level.
`thrill-dd` | WT construction using domain decomposition.
`thrill-flat-dd` | WT construction using non-recursive domain decomposition.
`thrill-sort` | WT construction using stable sorting on each level. This algorithm uses superscalar sample sort, which is problematic with a low number of distinct sort keys. This scenario occurs on the upper levels of the wavelet tree.
`thrill-wm-concat` | WM construction using bucket concatenation, i.e. bucket sorting with two buckets on each level.

### Helpers
These tools are not necessarily related to wavelet tree construction but have been written and used throughout development and writing of the thesis.

Binary | Description
------ | -----------
`alloc-test` | Tests how much memory can be allocated and written on the system.
`histogram` | Computes and prints the input's histogram.
`process` | Processes a text file, e.g., to filter only certain characters.
`thrill-input` | Small sanity benchmark of force-loading a file in Thrill. Related to [Thrill issue #188](https://github.com/thrill/thrill/issues/188).
`verify-levelwise` | Verifies a levelwise WT on disk against a source file. Implemented using Thrill.

The verifier simulates an *access* operations on all positions of the input text and compares the results to the source file, thus verifying the correctness of the wavelet tree. Note that currently, the verifier must use the same amount of workers that were used to generate the output.

## Output format
If output to disk is desired (`-o` parameter), the wavelet tree or matrix will be emitted in levelwise representation along with the histogram of the input text. In the following, we suppose the output filename to be `wt`.

### Histogram
The file `wt.hist` contains the histogram of the original input. It consists of a `size_t` representing the amount of histogram entries (i.e., the alphabet size), followed by (`unsigned char`, `size_t`) tuples, mapping each symbol occuring in the input to the amount of its occurences.

### Levelwise WT
For each level, you will find multiple files named like `wt${WORKER_ID}.lv_${LEVEL}`, where `${LEVEL}` is the WT's level the file contains, and `${WORKER_ID}` is the ID of the worker (MPI or Thrill) that produced the file. All file should be of the same size except possibly that emitted by the final worker.

Each of these files contains a bit vector stored as sequences of little endian encoded `uint64_t` values in MSBF order (i.e., the mots significant bit is set using `1 << 63`), which allows it to be "read" using `xxd -b` for debugging purposes on small instances.

The concatenation of all the files for the same level equals the level's full bit vector. Note that no alignment information is stored explictly; the length of the original input (and thus the length of the bit vectors) can be retrieved using the histogram.
