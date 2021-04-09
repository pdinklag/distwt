#!/bin/bash -l

#SBATCH --job-name=distwt
#SBATCH --partition=short
#SBATCH --time=20
#SBATCH --mem=49152
#SBATCH --exclusive
#SBATCH --nodes=16
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=20
#SBATCH --constraint=cstd01

export WORK=/work/$USER
export HOME=/home/$USER
export JOB_WD=work-$SLURM_JOB_ID
export WD=/scratch/$JOB_WD

srun mkdir -p $WD
srun mkdir -p $WD/thrill
srun cd $WD
cd $WD

export THRILL_WORKERS_PER_HOST=20
#export THRILL_LOG=$WD/thrill/log
$HOME/thrill/run/slurm/invoke.sh $@

#export OUT_WD=$WORK/$JOB_WD
#mkdir -p $OUT_WD
#echo "Moving results to $OUT_WD ..."
#srun rsync -r --ignore-existing $WD $WORK

