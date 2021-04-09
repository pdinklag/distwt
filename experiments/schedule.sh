#!/bin/bash

# --- SLURM CONFIG ---
WORK=/work/smpadink                 # result output directory
BIN=/home/smpadink/distwt/build/bin # binaries

# --- EXPERIMENTS ---
MPI_APPS=("$BIN/mpi-dd" "$BIN/mpi-wm-dd" "$BIN/mpi-bsort" "$BIN/mpi-dynbsort"  "$BIN/mpi-wm-concat" "$BIN/mpi-dsplit" "$BIN/mpi-wm-dsplit")
#THRILL_APPS=("$BIN/thrill-dd $BIN/thrill-bsort $BIN/thrill-sort")
THRILL_APPS=()

MPI_SLURM="mpi5.slurm"
THRILL_SLURM="thrill.slurm"

ITERATIONS=1

# input files
DATASETS=$WORK/datasets/large
FILES=("$DATASETS/dna.txt" "$DATASETS/commoncrawl.txt" "$DATASETS/wiki.txt" "$DATASETS/ru.wo_punct.wb" "$DATASETS/commoncrawl.txt.128Gi.sa5")
FILE_BPS=(1 1 1 4 5)            # number of bytes per symbol (BPS) for each of the input files
FILE_FLAGS=("" "" "" "-e" "-e") # the -e flag tells that the input is already in its effective alphabet

# experiment mode
MODE=$1

if [ "$MODE" == "weak" ]; then
    echo weak scaling
    CFG_NODES=(  1   2   4   8   16   24   32   40   48   56   64   72   80   88   96   )
    CFG_PREFIX=( 1Gi 2Gi 4Gi 8Gi 16Gi 24Gi 32Gi 40Gi 48Gi 56Gi 64Gi 72Gi 80Gi 88Gi 96Gi )
    OUTPUT="/work/smpadink/weak-%j.log"
elif [ "$MODE" == "breakdown" ]; then
    echo breakdown
    CFG_NODES=(  4   4   4   4    4    4    4     4  )
    CFG_PREFIX=( 1Gi 2Gi 4Gi 8Gi 16Gi 32Gi 64Gi 128Gi )
    OUTPUT="/work/smpadink/breakdown-%j.log"
elif [ "$MODE" == "cost" ]; then
    echo COST
    CFG_NODES=(   1    2    3    4    5    6    7    8  )
    CFG_PREFIX=( 16Gi 16Gi 16Gi 16Gi 16Gi 16Gi 16Gi 16Gi)
    OUTPUT="/work/smpadink/cost-%j.log"
else
    echo "usage: $0 <weak | breakdown | cost>"
fi

# -----------------

for ((k=0;k<${#CFG_NODES[@]};k++)); do
  NODES=${CFG_NODES[$k]}
  PREFIX=${CFG_PREFIX[$k]}

  for f in ${!FILES[@]}; do
    FILE=${FILES[$f]}
    FILENAME=`basename $FILE`
    BPS=${FILE_BPS[$f]}
    FFLAGS="${FILE_FLAGS[$f]}"

    for ((i=0;i<$ITERATIONS;i++)); do
      # MPI
      for ((a=0;a<${#MPI_APPS[@]};a++)); do
        APP="${MPI_APPS[$a]}"
        CMD="sbatch --output=$OUTPUT --nodes=$NODES $MPI_SLURM $APP $FFLAGS -p $PREFIX -w $BPS $FILE"
        echo $CMD
      done

      # THRILL
      for APP in $THRILL_APPS; do
        CMD="sbatch --nodes=$NODES $THRILL_SLURM $APP -p $PREFIX $FILE"
        echo $CMD
      done
    done
  done
done

