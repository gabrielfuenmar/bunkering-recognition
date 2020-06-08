# Bunkering Recognition Algorithm
Generates bunkering information from raw AIS data at the Mediterranean Sea from January, 2013 to June, 2019.
[Pseudocode](https://github.com/gabrielfuenmar/bunkering-recognition/blob/master/pseudo.pdf) available at the repository.

Distributed computing setting under Sun Grid manager deployed in a Round Robin configuration see bunker_mpi.sh.

MPI setting recognizes any distributed environment.

AIS information used as input and Vessel Specifications are not displayed here as restrictred by the suppliers.

[Results](https://github.com/gabrielfuenmar/bunkering-recognition/blob/master/bunkering_ops_mediterranean.csv) from the Mediterranean Operations does not shows the statistic population as the algorithm is dependent in the quality of AIS sequence. However, validation of the results can be observed at ##PAPER WHEN PUBLISHED###.

Dependencies:

    pandas 0.25.1
    geopandas 0.6.1
    shapely 1.6.4
    numpy 1.17.2
    mpi4py  3.0.2

Parameters:
        
    alongside_poly: geopandas dataframe with manually constructed polygons off shore.
    port_barge: pandas dataframe with match of ports and servicing barges.
    bunker_poly: geopandas dataframe of ports polygons.
    done: pandas dataframe from reaaded list at done.txt file. Initially empty.
    vessels_all: pandas dataframe with vessels specs. Not uploaded as restricted by the supplier.
    positions: pandas dataframe of vessels positions. Not uploaded as restricted by the supplier.

Returns:
    
    CSV file with bunkering statistics.

The information is build per vessel based on the correct sequence of three operations (Bunker barges recognition [Results as port_barge dataframe], Stopped Vessel and Vessel-Barge rendezvous).

A rough visual description of the algorithm looks as follows:
1. Port filter

![alt text](https://github.com/gabrielfuenmar/bunkering-recognition/blob/master/1_port_filter.png?raw=true)

2. Stopped vessel at anchor 

![alt text](https://github.com/gabrielfuenmar/bunkering-recognition/blob/master/2_anchoring_detection.png?raw=true)

3. Vessel-barge rendezvous

![alt text](https://github.com/gabrielfuenmar/bunkering-recognition/blob/master/3_vessel_barge_rendezvous.png?raw=true)


Credits: Gabriel Fuentes Lezcano
Licence: MIT License

Copyright (c) 2020 Gabriel Fuentes

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. © 2020 GitHub, Inc.

    
    
    
