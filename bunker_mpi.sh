
#!/bin/bash
#
#$ -cwd
#$ -j y
#$ -S /bin/bash

mpirun --mca btl vader,self,tcp  --map-by node -n 306 ~/anaconda3/envs/bunker2/bin/python /home/gabriel/codes/bunkering_all_med/barge_alongside_ship_all_med_cluster_new_DBS.py