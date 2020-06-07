"""
Created on Tue Oct 15 08:56:43 2019

Author: Gabriel Fuentes Lezcano
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import *
import numpy as np
from mpi4py import MPI
import random
import mydbscan as mdb

##Chunk list in rouglhy even parts
def chunk(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

##MPI machinery
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name=MPI.Get_processor_name()

if rank==0:
    ##alongside_polygon
    alongside_poly=gpd.read_file("med_black_100m_in.geojson")

    columns=["barge_imo","barge_mmsi","start_of_service","end_of_service","service_time","vessel_served_imo","vessel_inside_port","vessel_out_port","call_id","bunkering_port","at_berth","alongside_var","draught","anch_lon","anch_lat"]
    df_base=pd.DataFrame(columns=columns).to_csv("bunker_operations.csv",mode="a",index=False)
    
    port_barge=pd.read_excel("bunker_port_barge_combination_all_med.xlsx",usecols=["new_PortCo","imo","mmsi"])
    list_of_barges_imo=port_barge[port_barge.imo !=0].imo.unique().tolist()
    list_of_barges_mmsi=port_barge[port_barge.imo ==0].mmsi.unique().tolist()
    
    #Bunker ports polygon
    bunker_poly=gpd.read_file("polygon_ports_bunker_med.geojson")
    bunker_poly.drop(columns=["PortId","BE PORT_NA","COUNTRY_NA","PortCode","COUNTRY","Lat","Long","count","Cluster","clusters_n","new_Port_1","new_PortNa"],inplace=True)
    bunker_poly.crs = {'init' :'epsg:4326'}
    ##Ports with records of found bunker barges
    port_barge_valid=port_barge.groupby("new_PortCo").count().index.tolist()
    ##Filter only those bunker ports with recognized barges
    bunker_poly=bunker_poly[bunker_poly.new_PortCo.isin(port_barge_valid)]
    
    ##Remove the processed vessels
    done = pd.read_csv('termina.txt', sep=" ", header=None)
    ter=done[0].tolist()
    vessels_all=pd.read_csv("vessels_all_specs_no_duplicates.csv")

##Remaining vessels(IMO). All vessels that are not in the list
    vessels_all=vessels_all[~vessels_all.imo.isin(ter)]
    
    vessels_all=vessels_all[~((vessels_all.imo.isin(list_of_barges_imo))|(vessels_all.mmsi.isin(list_of_barges_mmsi))|(vessels_all.length<100)|(vessels_all.length.isnull()))]
    mask_imo_mmsi=vessels_all.imo.isnull()
    imo=vessels_all[~mask_imo_mmsi].imo.unique().tolist()
    imo=[int(i) for i in imo]
    random.shuffle(imo)
    imo=chunk(imo,size-1)
#    mmsi=vessels_all[mask_imo_mmsi].mmsi.unique().tolist()
#    mmsi=chunk(mmsi,size)

else:
    imo=None
#    mmsi=None
    port_barge=None
    bunker_poly=None
    alongside_poly=None
   
#mmsi=comm.scatter(mmsi,root=0)
port_barge=comm.bcast(port_barge,root=0)
bunker_poly=comm.bcast(bunker_poly,root=0)
alongside_poly=comm.bcast(alongside_poly,root=0)
imo=comm.scatter(imo,root=0)

print("Processor", name, "Rank", rank, imo) 

for ship in imo:
    
    meters=500 #Movement allowed in every half an hour. Speed of 0.5 knots max.
    m_eps_anchor=(meters/1852)/60
    
    df_values=[]
    positions=pd.read_csv("/home/gabriel/Ships_Position_Med/{}.csv".format(ship),usecols=["imo","mmsi","timestamp_position","lon","lat","speed","draught"])            
    positions["timestamp_position"]=pd.to_datetime(positions["timestamp_position"],format="%Y-%m-%d %H:%M:%S")
     ##To assute values are sorted by date
    positions.sort_values(by='timestamp_position',inplace=True)
     ##Geodataframe of csv with lon and lat as Points
    positions_gdf=gpd.GeoDataFrame(positions,geometry=[Point(x,y) for x,y in zip(positions.lon,positions.lat)])
    positions_gdf.crs = {'init' :'epsg:4326'}
    ##Spatial join of vessel to suez polygons
    port_stops=gpd.sjoin(positions_gdf,bunker_poly,how="left")
    ##Calculates the percentage of port positions within the data
    percentage_at_bunker_area=port_stops.new_PortCo.value_counts(normalize=True)/port_stops.shape[0]
        
    ##If a single port has more than 35% visit of the overrall dataset, then this is an inland vessel
    ##Second condition is no empty dataframe meaning no match
    if percentage_at_bunker_area.sum()<0.35 and port_stops.new_PortCo.notnull().any()==True:
        ##Remove duplicates
        port_stops=port_stops.drop_duplicates(subset=["timestamp_position"])
        #Subgroup of records based in their change in/out polygon
        port_stops['subgroup'] = (port_stops['WPI PORT_N'] != port_stops['WPI PORT_N'].shift(1)).cumsum()
        ##Removal positions with no record of visiting a port
        port_stops=port_stops[~port_stops["WPI PORT_N"].isnull()]
        #Vessels with less than 1 knots are anchored or stopped
        port_stops=port_stops[port_stops.speed<=1]
        ##Drop duplicates
        port_stops=port_stops.drop_duplicates()
        ##Keep only groups with more than 1 observation
        mask_subgroup=port_stops.subgroup.value_counts()>3
        list_of_valid_groups=mask_subgroup.index[port_stops.subgroup.value_counts()>3].tolist()
        list_of_valid_groups.sort()
        port_stops=port_stops[port_stops.subgroup.isin(list_of_valid_groups)]
        port_stops.drop(columns=["index_right"],inplace=True)      
        #Unique list of subgroups to index different calls at bunker ports 
        if port_stops.shape[0]!=0:
            ##Groupby every call
            index_call=port_stops.subgroup.unique().tolist()
            port_stops_p_group=port_stops.groupby("subgroup")     
            for callid in index_call:
                port_stops_in=port_stops_p_group.get_group(callid)
                ##Vessel with more than 5000 points is in layover. 3 months
                if port_stops_in.shape[0]<5000:
                    ##Geometry to array to insert into dbscan
                    X = [np.array([port_stops_in.lon.iloc[x],port_stops_in.lat.iloc[x]]) for x in range(len(port_stops_in))]
                    
                    clustering_labels=mdb.MyDBSCAN(X,eps=m_eps_anchor,MinPts=3)
#                    alongside_var=np.var(np.array(X),axis=0)
#                    alongside_var=(alongside_var < 9e-5).all() ##Less than 10 meters moment. Variance issue with sample size
                    port_stops_in=port_stops_in.assign(anchoring_cluster=clustering_labels)
                    port_stops_in=port_stops_in[port_stops_in.anchoring_cluster!=-1]
                    if port_stops_in.shape[0]!=0:
                        entry_time=port_stops_in.timestamp_position.iloc[0]
                        exit_time=port_stops_in.timestamp_position.iloc[-1]
                        time_test=(exit_time-entry_time)/np.timedelta64(1,'h')
                        ##Check if the visit last longer than 1 hour otherwise is a passing vessel
                        if time_test > 1:
                            ##Check if position is at anchor or alongside
                            port_stops_in=gpd.sjoin(port_stops_in,alongside_poly,how="left")
                            alongside_pos=port_stops_in.index_right.notnull().any()
                            port_stops_in.drop(columns=["index_right"],inplace=True) 
                            ##Use the barges that are temptative bunker barges
                            port_visited=port_stops_in.new_PortCo.iloc[-1]
                            barges_to_test=port_barge[port_barge["new_PortCo"]==port_visited]
                            ###Create a buffer polygon of 0.005 10m of every vessel position to test if the barge was close in a time period
                            RADIUS_FILTER=100/1852/60
                            line_anchoring=LineString([port_stops_in.geometry.iloc[x] for x in range(len(port_stops_in))])
                            line_buffer=line_anchoring.buffer(RADIUS_FILTER)
                            port_stops_in=gpd.GeoDataFrame([[port_stops_in.imo.iloc[-1],port_stops_in.mmsi.iloc[-1],
                                                           port_stops_in.draught.iloc[-1],port_stops_in["WPI PORT_N"].iloc[-1],
                                                           port_stops_in.new_PortCo.iloc[-1],line_buffer]],columns=["imo","mmsi","draught","WPI PORT_N",
                                                                                        "new_PortCo","geometry"],geometry="geometry")
                           
                            ##Unique values labelled by imo or if no imo available, by mmsi
                            list_of_barges_imo=barges_to_test[barges_to_test.imo !=0].imo.unique().tolist()
                            list_of_barges_mmsi=barges_to_test[barges_to_test.imo ==0].mmsi.unique().tolist() 
                        
                            #For loop with every mmsi labelled barge
                            for barges in list_of_barges_mmsi:
                                positions_barge=pd.read_csv("/home/gabriel/Ships_Position_Med/mmsi/m{}.csv".format(barges),usecols=["timestamp_position","lon","lat"])
                                ##Assure timestamp in proper format
                                positions_barge["timestamp_position"]=pd.to_datetime(positions_barge["timestamp_position"],format="%Y-%m-%d %H:%M:%S")
                                ##Filter barge positions inside date range
                                mask_date=(positions_barge.timestamp_position>=entry_time)&(positions_barge.timestamp_position<=exit_time)
                                positions_barge=positions_barge.loc[mask_date]
                                ##Geodataframe with geometry Point for every barge position. Test to avoid empty dataframes
                                if positions_barge.shape[0]>=1:
                                    positions_barge=gpd.GeoDataFrame(positions_barge,geometry=[Point(x,y) for x,y in zip(positions_barge.lon,positions_barge.lat)]) 
                                    ##Setting CRS uniform
                                    positions_barge.crs = {'init' :'epsg:4326'}
                                    port_stops_in.crs = {'init' :'epsg:4326'}
                                    merge_test=gpd.sjoin(positions_barge,port_stops_in,how="left")
                                    any_match=merge_test.index_right.notnull().any()
                                else:
                                    any_match=False                    
                                #Test: If more than 0 mathces are found then go into the following instructions
                                if any_match==True:   
                                    #Subgroup of alongside positions to vessel
                                    ##Guarantees subsequent points as a visit
                                    merge_test['subgroup'] = (merge_test['WPI PORT_N'] != merge_test['WPI PORT_N'].shift(1)).cumsum()
                                    if merge_test.shape[0]>=0:
                                        ##Creates a list of unique subgroups id's
                                        merge_test_index=merge_test.subgroup.unique().tolist()
                                        #Divides the visit to our vessel by groups if not continuous
                                        inner_merger_test=merge_test.groupby("subgroup")
                                        ##Loops thorugh the subgroups to calculate times of service
                                        for index_merger in merge_test_index:
                                            loop_subvisits_barge_df=inner_merger_test.get_group(index_merger)
                                            ##First vtimestamp of subgroup
                                            start_service=loop_subvisits_barge_df.timestamp_position.iloc[0]
                                            ##Last timestamp of subgroup
                                            end_of_service=loop_subvisits_barge_df.timestamp_position.iloc[-1]
                                            ##Service time is from the bunker barge inside the vessel radius until it is out of the radius
                                            service=(end_of_service-start_service)/np.timedelta64(1,'h')
                                            visited_port=loop_subvisits_barge_df["WPI PORT_N"].iloc[-1]
                                            
                                            draught_anch=loop_subvisits_barge_df.draught.iloc[-1]
                                            random_row=loop_subvisits_barge_df.sample()
                                            anch_lon=random_row.lon.iloc[0]
                                            anch_lat=random_row.lat.iloc[0]
                                            ##If the service time is higher than 1 hour(Bunker barge in close vicinity of vessel), then this was an official service
                                            if service >1: ##Any continuous visit of more than 1 hour is a service
                                                ##All the rsulting data is attached to a dict of list, readable by pandas
                                                df_values.append([np.nan,barges,start_service,end_of_service,service,ship,entry_time,exit_time,
                                                            callid,visited_port,alongside_pos,np.nan,draught_anch,anch_lon,anch_lat])
        
                            for barges in list_of_barges_imo:
                                positions_barge=pd.read_csv("/home/gabriel/Ships_Position_Med/{}.csv".format(barges),usecols=["timestamp_position","lon","lat"])
                                ##Assure timestamp in proper format
                                positions_barge["timestamp_position"]=pd.to_datetime(positions_barge["timestamp_position"],format="%Y-%m-%d %H:%M:%S")
                                ##Filter barge positions inside date range
                                mask_date=(positions_barge.timestamp_position>=entry_time)&(positions_barge.timestamp_position<=exit_time)
                                positions_barge=positions_barge.loc[mask_date]
                                ##Geodataframe with geometry Point for every barge position. Test to avoid empty dataframes
                                if positions_barge.shape[0]>=1:
                                    positions_barge=gpd.GeoDataFrame(positions_barge,geometry=[Point(x,y) for x,y in zip(positions_barge.lon,positions_barge.lat)]) 
                                    ##Setting CRS uniform
                                    positions_barge.crs = {'init' :'epsg:4326'}
                                    port_stops_in.crs = {'init' :'epsg:4326'}
                                    merge_test=gpd.sjoin(positions_barge,port_stops_in,how="left")
                                    any_match=merge_test.index_right.notnull().any()
                                else:
                                    any_match=False                    
                                #Test: If more than 0 mathces are found then go into the following instructions
                                if any_match==True:
       
                                    #Subgroup of alongside positions to vessel
                                    ##Guarantees subsequent points as a visit
                                    merge_test['subgroup'] = (merge_test['WPI PORT_N'] != merge_test['WPI PORT_N'].shift(1)).cumsum()
                                    ##Removes positions not close to our vessel
                                    if merge_test.shape[0]>=0:
                                        ##Creates a list of unique subgroups id's
                                        merge_test_index=merge_test.subgroup.unique().tolist()
                                        #Divides the visit to our vessel by groups if not continuous
                                        inner_merger_test=merge_test.groupby("subgroup")
                                        ##Loops thorugh the subgroups to calculate times of service
                                        for index_merger in merge_test_index:
                                            loop_subvisits_barge_df=inner_merger_test.get_group(index_merger)
                                            ##First vtimestamp of subgroup
                                            start_service=loop_subvisits_barge_df.timestamp_position.iloc[0]
                                            ##Last timestamp of subgroup
                                            end_of_service=loop_subvisits_barge_df.timestamp_position.iloc[-1]
                                            ##Service time is from the bunker barge inside the vessel radius until it is out of the radius
                                            service=(end_of_service-start_service)/np.timedelta64(1,'h')
                                            visited_port=loop_subvisits_barge_df["WPI PORT_N"].iloc[-1]
                                            draught_anch=loop_subvisits_barge_df.draught.iloc[-1]
                                            random_row=loop_subvisits_barge_df.sample()
                                            anch_lon=random_row.lon.iloc[0]
                                            anch_lat=random_row.lat.iloc[0]
                                            
                                            ##If the service time is higher than 1 hour(Bunker barge in close vicinity of vessel), then this was an official service
                                            if service >1: ##Any continuous visit of more than 1 hour is a service
                                                ##All the rsulting data is attached to a dict of list, readable by pandas
                                                df_values.append([barges,np.nan,start_service,end_of_service,service,ship,entry_time,exit_time,
                                                            callid,visited_port,alongside_pos,np.nan,draught_anch,anch_lon,anch_lat])


    df_export=pd.DataFrame(df_values)  
    df_export.to_csv("bunker_operations.csv",mode="a",index=False,header=False) 
    
    f1= open("termina.txt","a+")
    ##Writes in the log
    f1.write("{}\n".format(ship))
    f1.close()
    





