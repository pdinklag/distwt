# Distributed Wavelet Tree Construction
This repository contains the code I wrote in the scope of my master's thesis on the distributed construction of wavelet trees (WT).

It is split up into two *worlds*: the Thrill world contains WT construction algorithms based on the [Thrill framework](http://project-thrill.org/), the MPI world is based on MPI. While Thrill allows for swift high-level sketching of algorithms which almost immediately works in practice, you will find a lot more low-level yada in the MPI part which may be useful for other projects, too (e.g. the parallel split operation).

## Building
The only hard dependencies are Thrill and MPI.

Pass `-DTHRILL_ROOT_DIR=<path/to/thrill>` to CMake pointing to a fully built clone of the [Thrill repository](https://github.com/thrill/thrill).

## Algorithms and Helpers
Here's a quick roundup of the generated binaries.

More details will follow in my master's thesis, which is due in February 2019 and will be published some time after.

### WT Construction
By default, the WT is constructed only in RAM (or semi-externally at Thrill's discretion). All binaries accept a `-o <file>` command line parameter to specify an output file, as well as the `-p <bytes>` parameter to tell them to process only that specified prefix of the input file. See below for details on the output.

Binary | Description
------ | -----------
`mpi-dd` | Domain decomposition in MPI.
`mpi-parsplit` | Weighted parallel split approach in MPI.
`thrill-dd` | Implicit domain decomposition in Thrill.
`thrill-flat-dd` | Implicit domain decomposition in Thrill (non-recursive).
`thrill-sort` | Stable sorting on each level in Thrill.

The `mpi` binaries accept the `-l <local_file>` parameter. If given, a worker's local part of the input file will be extracted to `local_file` in a preliminary step, so "chaotic" parallel access to the input, which may have heavy hits on the performance, can be avoided.

### Helpers
Binary | Description
------ | -----------
`alloc-test` | Tests how much memory can be allocated and written on the system.
`histogram` | Computes and prints the input's histogram.
`process` | Processes a text file, e.g., to filter only certain characters.
`verify-levelwise` | Verifies a levelwise WT on disk against a source file.

The verifier simulates an *access* operations on all positions of the input text and compares the results to the source file, thus verifying the correctness of the wavelet tree. Note that currently, the verifier must use the same amount of workers that were used to generate the output.

## WT output format
If output to disk is desired (`-o` parameter), the wavelet tree will be emitted in levelwise representation along with the histogram of the input text. In the following, we suppose the output filename to be `wt`.

### Histogram
The file `wt.hist` contains the histogram of the original input. It consists of a `size_t` representing the amount of histogram entries (i.e., the alphabet size), followed by (`unsigned char`, `size_t`) tuples, mapping each symbol occuring in the input to the amount of its occurences.

### Levelwise WT
For each level, you will find multiple files named like `wt${WORKER_ID}.lv_${LEVEL}`, where `${LEVEL}` is the WT's level the file contains, and `${WORKER_ID}` is the ID of the worker (MPI or Thrill) that produced the file. All file should be of the same size except possibly that emitted by the final worker.

Each of these files contains a bit vector stored as sequences of little endian encoded `uint64_t` values in MSBF order (i.e., the mots significant bit is set using `1 << 63`), which allows it to be "read" using `xxd -b` for debugging purposes on small instances.

The concatenation of all the files for the same level equals the level's full bit vector. Note that no alignment information is stored explictly; the length of the original input (and thus the length of the bit vectors) can be retrieved using the histogram.
