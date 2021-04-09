#!/bin/bash -l

#SBATCH --job-name=distwt
#SBATCH --partition=short
#SBATCH --time=50
#SBATCH --mem=49152
#SBATCH --nodes=16
#SBATCH --ntasks-per-node=20
#SBATCH --cpus-per-task=1
#SBATCH --constraint=cstd01

export WORK=/work/$USER
export HOME=/home/$USER
export JOB_WD=work-$SLURM_JOB_ID
export WD=/scratch/$JOB_WD

echo $@
echo

# first iteration
mpirun $@

RET=$?
if [ $RET -ne 0 ]; then
    echo "First iteration failed (exit code $RET), won't even try the others."
    exit
fi

# following iterations
mpirun $@
mpirun $@
mpirun $@
mpirun $@

