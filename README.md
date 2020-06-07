# Bunkering Recognition Algorithm
Generates bunkering information at the Mediterranean Sea from raw AIS data.

Distributed computing setting under Sun Grid manager deployed in a Round Robin configuration see bunker_mpi.s for Sun Grid setting.

AIS information used as input and Vessel Specifications are not displayed here as restrictred by the suppliers.

Results from the Mediterranean Operations does not cover the population as the algorithm is dependent in the quality of AIS sequence. However, validation of the results can be observed at ##PAPER WHEN PUBLISHED###.

Dependencies:

    pandas 0.25.1
    geopandas 0.6.1
    shapely 1.6.4
    numpy 1.17.2
    mpi4py  3.0.2


Parameters:
        
    alongside_poly: geopandas dataframe with manually constructed polygons off         shore.
    port_barge: pandas dataframe with match of ports and servicing barges.
    bunker_poly: geopandas dataframe of ports polygons.
    done: pandas dataframe from reaaded list at done.txt file. Initially               empty.
    vessels_all: pandas dataframe with vessels specs. Not uploaded as                 restricted by the supplier.
    positions: pandas dataframe of vessels positions. Not uploaded as                 restricted by the supplier.


