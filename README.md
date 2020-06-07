# Bunkering Recognition Algorithm
Generates bunkering information at the Mediterranean Sea from raw AIS data.

Distributed computing setting under Sun Grid manager deployed in a Round Robin configuration see bunker_mpi.sh.

AIS information used as input and Vessel Specifications are not displayed here as restrictred by the suppliers.

Results from the Mediterranean Operations doesnot cover the population as the algorithm is dependent in the quality of AIS sequence. However, validation of the results can be observed at ##PAPER WHEN PUBLISHED###.

Dependencies:

    pandas 0.25.1
    geopandas 0.6.1
    shapely 1.6.4
    numpy 1.17.2
    mpi4py  3.0.2




