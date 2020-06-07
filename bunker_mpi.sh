
#!/bin/bash
#
#$ -cwd
#$ -j y
#$ -S /bin/bash

mpirun --mca btl vader,self,tcp  --map-by node -n 306 ~python barge_alongside_ship_all_med_cluster_new_DBS.py
