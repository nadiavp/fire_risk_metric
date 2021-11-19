import opendssdirect as odd
import sys
import pathlib
import pyproj
import json
import pandas as pd
from scipy.io import netcdf
import netCDF4
import gzip
import pyarrow.parquet as pq
import geojson
import numpy as np
from sklearn.neighbors import BallTree
# this script runs through the opendss files of a power system model
# and applies the fire risk metric which quantifies the risk of the
# power system igniting a fire
dist_trait_names = ['line_to_veg_dist', 'line_to_line_dist', 'line_to_gnd_dist', 'line_age',
                    'transformer_age', 'oil_type_transformer', 'overhead', 'uninsulated',
                    'line_peak_load', 'transformer_peak_load', 'hif_detection', 'powersafety_shutoff',
                    'misting_fire_suppresion', 'response_team_coordination', 'high_fidelity_tracking']

terrain_trait_names = ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']

def get_dist_risk_from_parquet(dist_risk_file):
    dist_risk_table = pq.read_table(dist_risk_file)
    dist_risk_df = dist_risk_table.to_pandas()
    print(dist_risk_df['p10uhs0_1247--p10udt2190'][0])
    return dist_risk_df

def get_dist_fire_traits(power_model, bus_coords):
    # check the risks we know just by looking at the distribution model 
    # default to 0
    n_traits = len(dist_trait_names)
    dist_fire_components = pd.read_csv(power_model) #get_dist_risk_from_parquet(power_model)
    #dist_risk_scores = {}
    ##### TODO: run model at peak load and export line currents and transformer currents
    # go through all components and check risk factors
    dfc_coords = []
    dfc_score = []
    for index, dfc_entry in dist_fire_components.iterrows():
        #dfc_entry = dist_fire_components[dfc]
        #dist_fire_components[index]['score'] = []
        #dist_fire_components[index]['coords'] = []
        #dist_fire_components[index]['coords'] = [0,0]
        dfc = dfc_entry['feeder']
        if dfc in bus_coords:
            #dist_fire_components[index]['coords'] = [bus_coords[dfc]]
            dfc_coords.append((bus_coords[dfc][0], bus_coords[dfc][1]))
        else:
            dfc_coords.append((38,-122.0)) 
        risk_score = [0]*n_traits
        overhead = False
        # check line features
        if dfc.startswith('Line'):
            # check age
            if dfc:
                risk_score[3] = 0
        percent_overhead = dfc_entry['percentage overhead']
        risk_score[6] = percent_overhead/10
        # check if insulated. True for low voltage in city or UG lines
        if 'lv' not in dfc_entry['feeder']:
            uninsulated = percent_overhead
            risk_score[7] = uninsulated/10
        # check line peak current
        risk_score[8] = dfc_entry['percentage line length overloaded']
        # check transformer loading
        risk_score[9] = dfc_entry['percentage transformers overloaded']
        # check if oil
        #if dfc: # assume all smart ds transformers are oil based
        risk_score[5] = 10
        # check resilience features
        # high impedance fault detection is still an area of research, power
        # safety shutoffs are only used in some areas of California
        # misting fire hose nozzles near high voltage lines were only applied in China
        # response team coordination is difficult to quantify
        # most fire response teams have 1km resolution sattelite tracking
        risk_score[10] = 10 # high impedance fault detection
        risk_score[11] = 5 # power safety shutoffs
        risk_score[12] = 10 # misting fire hose nozzle
        risk_score[13] = 5 # response team coordination
        risk_score[14] = 1 # high fidelity tracking
        # add risk score to json
        #dist_fire_components[index]['score'] = risk_score
        dfc_score.append(risk_score)
    dist_fire_components['score'] = dfc_score
    dist_fire_components['coords'] = dfc_coords
    # export distribution risk score to json format
    #with open('dist_risk_scores.json', 'w') as dist_score_file:
    #    geojson.dump(dist_fire_components, dist_score_file)
    dist_fire_components.to_csv('dist_risk_scores.csv', index=False)
    return dist_fire_components

def get_dist_fire_locations(power_model_dir):
    # parse the files and select only the bay area: as defined as
    # north to Sacremento: 38.5816N 121.4944W, 
    # south and east to Fresno: 36.7378N 119.7871W, 
    # and west to longitude 120W
    dist_coord_frame = pyproj.Proj(init='epsg:32610')
    lon_lat_coord_frame = pyproj.Proj(init='epsg:5070')
    dist_fire_components = {} # add something to read all from model
    region_name = str(power_model_dir).split('/')[-1]
    if region_name == str(power_model_dir):
        dir_char = "\\"
        region_name = str(power_model_dir).split(dir_char)[-1]
    print(f'getting coordinates from {region_name} at {power_model_dir}')
    sub_regions = []
    potential_sub_regions = power_model_dir.glob('*')
    for folder in potential_sub_regions:
        sub_region_name = str(folder).split(dir_char)[-1]
        if sub_region_name.startswith(region_name) and (folder / "DSSfiles/Buscoords.dss").exists() and (folder / "DSSfiles/Transformers.dss").exists():
            sub_regions.append(folder)
            trans_file = (folder / "DSSfiles/Transformers.dss")
            line_file = (folder / "DSSfiles/Lines.dss")
            bus_file = (folder / "DSSfiles/Buscoords.dss")
            linecode_file = (folder / "DSSfiles/LineCodes.dss")
            # read in bus coordinates
            bus_coords = {}
            with open(bus_file, 'r') as bf:
                bus_coord_lines = bf.readlines()
                for line in bus_coord_lines:
                    if len(line)>4:
                        bus_entry = line.split(' ')
                        bus_name = bus_entry[0]
                        bus_x = bus_entry[1]
                        bus_y = bus_entry[2]
                        bus_coord = pyproj.transform(dist_coord_frame, lon_lat_coord_frame, bus_x, bus_y) 
                        bus_coords[bus_name] = bus_coord
            # read in line codes
            line_codes = {}
            with open(linecode_file, 'r') as lcf:
                lcf_lines = lcf.readlines()
                for line in lcf_lines:
                    if len(line)>4:
                        code_entry = line.split(' ')
                        code_name = code_entry[1].replace('Linecode.', '')
                        normamps = code_entry[-1].replace('normamps=', '')
                        line_codes[code_name] = normamps
            # read in lines
            with open(line_file, 'r') as lf:
                lf_lines = lf.readlines()
                for line in lf_lines:
                    if len(line.split('bus1='))>1:#len(line)>4:
                        #if 'bus1=' in line: #len(line)>4:
                        line_entry = line.split(' ')
                        name = line_entry[1]
                        bus1 = line.split('bus1=')[1].split(' ')[0].split('.')[0] #line_entry[3].replace('bus1=', '').split('.')[0]
                        if bus1[-1]=='x':
                            bus1 = bus1[:-1]
                        bus2 = line.split('bus2=')[1].split(' ')[0].split('.')[0] #line_entry[4].replace('bus2=', '').split('.')[0]
                        if bus2[-1]=='x':
                            bus2 = bus2[:-1]
                        linecode = line.split('Linecode=')[1].split(' ')[0].replace('\n','') #line_entry[-1].replace('Linecode=', '')
                        # add info to dictionary
                        if bus1 in bus_coords.keys() and bus2 in bus_coords.keys():
                            dist_fire_components[name] = {}
                            dist_fire_components[name]['coords'] = [bus_coords[bus1]]
                            dist_fire_components[name]['coords'].append(bus_coords[bus2])
                            dist_fire_components[name]['normamps'] = line_codes[linecode]
            # read in transformers
            # with open(trans_file, 'r') as tf:
            #     tf_lines = tf.readlines()
            #     for line in tf_lines:
            #         if 'bus=' in line:
            #             line_entry = line.split(' ')
            #             name = line_entry[1]
            #             bus = line.split('bus=')[1].split(' ')[0].split('.')[0]#line_entry[8].replace('bus=', '')
            #             # add info to dictionary
            #             dist_fire_components[name] = {}
            #             dist_fire_components[name]['coords'] = [bus_coords[bus]]
            #             dist_fire_components[name]['normamps'] = 1000000000 # need to find a way to add rating
    return dist_fire_components, bus_coords

def get_terrain_coords(soil_file, soil_moisture_file, veg_file, lightning_file, burnprob_file):
    # parse the files and select only the bay area: as defined as
    # north to Sacremento: 38.5816N 121.4944W, 
    # south and east to Fresno: 36.7378N 119.7871W, 
    # and west to longitude 120W (west is negative?)
    terrain_component_coords = {}
    terrain_component_coords["type"] = "FeatureCollection"
    terrain_component_coords["features"] = []

    # get table for soil saturation
    soil_table = pd.read_csv(soil_file)
    soil_table = soil_table[['Region', 'Latitude', 'Longitude', 'Species', 'Soil_type', 'Soil_drainage',
                            'Ecosystem_type', 'Ecosystem_state', 'Leaf_habit']]
    soil_table = soil_table[soil_table['Region']=='California']
    moisture_table = pd.read_csv(soil_moisture_file)
    moisture_table = moisture_table[['date', 'time', 'latitude', 'longitude', 'soil_moisture']]
    #for sd, lat, long in zip(soil_table['Soil_drainage'], soil_table['Latitude'], soil_table['Longitude']):#soil_table['Soil_type']):
    #    # not sure we need anything for soil type
    #    if sd=='Dry':
    #        risk = 10
    #    elif sd=='Wet':
    #        risk = 0
    #    else:
    #        risk=0
    moisture_risk_list=[]
    soil_risk_list = []
    for sm, lat, long, date, time in zip(moisture_table['soil_moisture'], moisture_table['latitude'], moisture_table['longitude'], moisture_table['date'], moisture_table['time']):
        risk = (1-float(sm))*10
        if risk == 'nan' or np.isnan(risk):
            print('nan risk for soil moisture')
            pass
        # add rainfall data interrelated with soil drainage to get overall saturation
        #rain_table = pd.read_csv(precip_file)
        
        # add soil risk to geojson
        if risk<=11 and risk>=0:
            soil_entry = {}
            soil_entry["type"]="Feature"
            soil_entry["geometry"] = {}
            soil_entry["geometry"]["type"] = "Point"
            soil_entry["geometry"]["coordinates"] = (lat, long)
            soil_entry["properties"]={}
            soil_entry["properties"]["soil_sat"]=risk
            soil_entry["properties"]["date"] = date
            soil_entry["properties"]["time"] = time
            moisture_risk_list.append(soil_entry)
            terrain_component_coords["features"].append(soil_entry)
    print('soil saturation risk added to json')
    # get table for lightning
    lf = netCDF4.Dataset(lightning_file)
    flashes = lf.variables['VHRMC_LIS_FRD'] # mean flashes for each month at long and lat in flashes/km2/day
    months, latitude, longitude = flashes.get_dims()
    months = lf.variables[months.name]
    latitude = lf.variables['Latitude']
    longitude = lf.variables['Longitude']
    print(f"months: {months[0]} to {months[-1]}, \
        lat: {latitude[0]} to {latitude[-1]}, \
        long: {longitude[0]} to {longitude[-1]} \
        resolution of {latitude[1]-latitude[0]} degrees,") #\
    #    flash range: {min(flashes)} to {max(flashes)}")
    # pull out for SFO bay area and convert to geojson
    flash_risk_list = []
    for month in months:
        month = float(month)
        lat_i = 0
        for lat in latitude:
            lat = float(lat)
            if lat>36.7378 and lat<38.5816:
                long_i = 0
                for long in longitude:
                    long = float(long)
                    if long>-120 and long<-119.7871:#119.7871 and long<120:
                        flash = flashes[int(month-1), lat_i, long_i] #[month][lat][long]
                        if flash == 'nan' or np.isnan(flash) :
                            print('nan risk for flashes')
                            pass
                        elif risk<=100 and risk>=0 and month<=12:
                            risk = flash/0.05 * 10 # normalize with 0.05 and then scale to 10 point scale
                            # add risk to geojson
                            lightning_entry = {}
                            lightning_entry["type"]="Feature"
                            lightning_entry["geometry"] = {}
                            lightning_entry["geometry"]["type"] = "Point"
                            lightning_entry["geometry"]["coordinates"] = (lat, long)
                            lightning_entry["properties"]={}
                            lightning_entry["properties"]["flash"]=risk
                            lightning_entry["properties"]["month"]=month
                            flash_risk_list.append(lightning_entry)
                            terrain_component_coords["features"].append(lightning_entry)
                    long_i = long_i+1
            lat_i = lat_i+1
    print('lightning risk added to json')
    # convert ecosystem type and state into fire risk value
    vegetation_risk = 0
    veg_risk_list = []
    for ess, est, lh, lat, long in zip(soil_table['Ecosystem_state'], soil_table['Ecosystem_type'], soil_table['Leaf_habit'], soil_table['Latitude'], soil_table['Longitude']):
        lat = float(lat)
        long = float(long)
        if ess == 'Managed':
            risk = 0
        elif ess == 'Unmanaged': #unmanaged means it was managed or disturbed by people in the past
            risk = 10
        else: # if it's natural
            risk = 5
        vegetation_risk=risk
            
        if est == 'Desert':
            risk = 10
        elif est == 'Savanna':
            risk = 9
        elif est == 'Grassland':
            risk = 8
        elif est == 'Shrubland':
            risk = 7
        elif est == 'Forest':
            risk = 3
        elif est == 'Agriculture':
            risk = 2
        else: # nan's 0s
            risk = 0
        vegetation_risk = (vegetation_risk + risk)/2
        for month in range(1,12):
            if lh =='Deciduous':
                if month>=9 and month<=11:
                    risk = 5
                else:
                    risk = 3
            else: # if evergreen
                risk = 5
            vegetation_risk = (vegetation_risk*2 + risk)/3
            if risk == 'nan' or np.isnan(risk):
                print('nan risk in vegetation')
            elif risk <=11 and risk>=0 and lat<39.0 and lat>36.0 and long>-122.0 and long<-118.0:
                # add risk to geojson
                veg_entry = {}
                veg_entry["type"]="Feature"
                veg_entry["geometry"] = {}
                veg_entry["geometry"]["type"] = "Point"
                veg_entry["geometry"]["coordinates"] = (lat, long)
                veg_entry["properties"]={}
                veg_entry["properties"]["veg_type"]=vegetation_risk
                veg_entry["properties"]["month"]=month
                veg_risk_list.append(veg_entry)
                terrain_component_coords["features"].append(veg_entry)
    print('vegetation type risk added to json')
    terrain_risk_no_nan = {}
    terrain_risk_no_nan["type"] = "Feature Collection"
    terrain_risk_no_nan["features"] = {}
    terrain_risk_no_nan["features"]["veg_type"] = []
    terrain_risk_no_nan["features"]["lightning"] = []
    terrain_risk_no_nan["features"]["soil_type"] = []
    terrain_risk_no_nan["features"]["soil_moisture"] = []
    terrain_risk_no_nan["features"]["extra"] = []
    for feature in terrain_component_coords["features"]:
        if np.isnan(feature["geometry"]["coordinates"][0]) or np.isnan(feature["geometry"]["coordinates"][1]):
            pass
        elif "date" in feature["properties"].keys() and not isinstance(feature["properties"]["date"], str): #np.isnan(feature["properties"]["date"]):
            pass
        elif "time" in feature["properties"].keys() and not isinstance(feature["properties"]["time"], str):
            pass
        elif "month" in feature["properties"].keys() and np.isnan(feature["properties"]["month"]):
                pass
        elif "veg_type" in feature["properties"].keys():
            if np.isnan(feature["properties"]["veg_type"]):
                pass
            else:
                terrain_risk_no_nan["features"]["veg_type"].append(feature)
        elif "flash" in feature["properties"].keys():
            if np.isnan(feature["properties"]["flash"]):
                pass
            else:
                terrain_risk_no_nan["features"]["lightning"].append(feature)
        elif "soil_sat" in feature["properties"].keys():
            if np.isnan(feature["properties"]["soil_sat"]):
                pass
            else:
                terrain_risk_no_nan["features"]["soil_moisture"].append(feature)
        else:
            terrain_risk_no_nan["features"]["extra"].append(feature)
    
    with open('terrain_risk_scores.json', 'w') as terr_score_file:
        json.dump(terrain_risk_no_nan, terr_score_file)
    print('terrain risk factors recorded in terrain_risk_scores.json')
    return terrain_component_coords, moisture_risk_list, flash_risk_list, veg_risk_list

def distribution_terrain_risk_matrix():
    dist_terr_cross_list = [['ground_veg', 'veg_moisture', 'wind_speed', 'ambient_temp'], # line-to-veg distange
                            ['ground_veg', 'wind_speed', 'ambient_temp'], # line-to-line distance
                            ['ground_veg', 'soil_saturation', 'wind_speed', 'ambient_temp'], # line-to-ground distance
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'], # line age
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'], # transformer age
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'], # oil type transformer
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed'], # overhead
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed'], # uninsulated
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed', 'ambient_temp'], # line peak load
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed', 'ambient_temp'], # transformer peak load
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'], # HIF detection
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'], # power safety shutoff threshold
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'], # provision of misting fire suppression equipment
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'], # fire response team coordination
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']] # high fidelity fire tracking
    return dist_terr_cross_list


def get_interrelated_risk(terrain_risks, dist_risks, soil_risk, lightning_risk, veg_risk, temperature_file, wind_file):
    # use nearest neighbor for each distribution feeder and each environmental factor, to get an interrelationship score for that feeder
    ######## find nearest neighbor indices
    # get coordinates for distribution feeders and all environmental factors
    all_coords = []
    print(f'dist_fire_risk keys: {dist_risks.keys()}')
    for dfr in dist_risks['coords']:
        all_coords.append(dfr)
        #if 'coords' in dist_risks[dfr].keys():
        #    dist_coord = dist_risks[dfr]['coords']
        #    all_coords.append(dist_coords)
    #all_coords = [dr for dr in dist_risks['coords']]
    
    print(all_coords[1:4])
    soil_coords = [sc['geometry']['coordinates'] for sc in soil_risk]
    light_coords = [lc['geometry']['coordinates'] for lc in lightning_risk]
    veg_coords = [vc['geometry']['coordinates'] for vc in veg_risk]
    
    #tree = BallTree(terrain_coords, leaf_size=15)
    #bus_terrain_distances, terrain_indices = tree.query(all_coords, k=1)
    soil_tree = BallTree(soil_coords, leaf_size=10)
    light_tree = BallTree(light_coords, leaf_size=10)
    veg_tree = BallTree(veg_coords, leaf_size=10)
    bus_soil_dist, soil_inds = soil_tree.query(all_coords, k=1)
    bus_light_dist, light_inds = light_tree.query(all_coords, k=1)
    bus_veg_dist, veg_inds = veg_tree.query(all_coords, k=1)
    
    #print(f'soil inds: {soil_inds}')
    #print(f'soil risk[0]: {soil_risk[soil_inds[0][0]]}')
    # go through nearest neighbor points and assign attributes
    risk_table = distribution_terrain_risk_matrix()
    dist_i = 0
    all_risks = {}
    for ind, dfr in dist_risks.iterrows():
        feeder_risks = []
        feeder_risks = dfr['score']
        all_risks[dfr['feeder']] = {}
        all_risks[dfr['feeder']]['risk'] = 0
        all_risks[dfr['feeder']]['coords'] = (38, -122)
        # ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']
        nearest_soil = soil_risk[soil_inds[dist_i][0]]['properties']['soil_sat']
        nearest_light = lightning_risk[light_inds[dist_i][0]]['properties']['flash']
        nearest_veg = veg_risk[veg_inds[dist_i][0]]['properties']['veg_type']
        # follow the matrix
        fri=0
        inter_risk = []
        for fr in feeder_risks:
            if 'ground_veg' in risk_table[fri]:
                inter_risk.append(fr*nearest_veg)
            if 'soil_saturation' in risk_table[fri]:
                inter_risk.append(fr*nearest_soil)
            #if 'veg_moisture' in risk_table[fri]:
            #    inter_risk.append(fr*nearest_moist)
            if 'lightning' in risk_table[fri]:
                inter_risk.append(fr*nearest_light)
            #if 'wind_speed' in risk_table[fri]:
            #    inter_risk.append(fr*nearest_wind)
        all_risks[dfr['feeder']]['risk']=sum(inter_risk)
        all_risks[dfr['feeder']]['coords'] = dfr['coords']
    return all_risks

def quantify_fire_risk(power_model_risk, fire_gis_dir, soil_file, soil_moisture_file, veg_file, lightning_file, burnprob_file, temperature_file, wind_file):
    # start with a fire risk of zero and slowly add to that
    total_fire_risk = 0
    # initialize list of terrain traits
    # establish fire risk correlations
    dist_terr_cross_list = distribution_terrain_risk_matrix()
    # load the power model
    #power_model = odd.run_command(str(power_model_dir / 'DSSfiles/Master.dss'))
    # get locations for power system components and assign locations to fire risk traits
    dist_fire_components, bus_coords = get_dist_fire_locations(power_model_dir)
    # get power model traits according to metric
    dist_fire_components = get_dist_fire_traits(power_model_risk, bus_coords)
    # get locations for fire susceptible attributes
    terrain_component_coords, soil_risk, lightning_risk, veg_risk = get_terrain_coords(soil_file, soil_moisture_file, veg_file, lightning_file, burnprob_file)
    # use nearest neighbor to find interdependent risk factors
    total_fire_risk = get_interrelated_risk(terrain_component_coords, dist_fire_components, soil_risk, lightning_risk, veg_risk, temperature_file, wind_file)
    
    return total_fire_risk

if __name__=="__main__":
    power_model_dir = pathlib.Path("C:/Users/npanossi/Documents/FireSEEDLDRD/metric_script/p28u")
    #("C:/Users/npanossi/Documents/Gemini-XFC/GEMINI-XFC/P1U/solar_medium_batteries_low_timeseries/DSSfiles/p1uhs0_1247")
    #sys.argv[1]
    if isinstance(power_model_dir, str):
        power_model_dir = pathlib.Path(power_model_dir)
    power_model_risk = 'smbl_summary.csv'
    fire_gis_dir = sys.argv[2]
    if isinstance(fire_gis_dir, str):
        fire_gis_dir = pathlib.Path(fire_gis_dir)
    soil_file = 'srdb-data-V5.csv'
    soil_moisture_file = 'SCAN_FullBayArea_20100901_20210901.csv'
    veg_file = 'cdl_fccs_merge2010/cdl_fccs_merge2010.tif.aux.xml'
    lightning_file = 'VHRMC.nc'
    burnprob_file = 'CONUS_iBP.tif'
    temperature_file = 'temperature.parquet'
    wind_file = 'wind.parquet'
    if isinstance(fire_gis_dir, str):
        fire_gis_dir = pathlib.Path(fire_gis_dir)
    soil_file = fire_gis_dir / soil_file
    soil_moisture_file = fire_gis_dir / soil_moisture_file
    print(soil_file)
    veg_file = fire_gis_dir / veg_file
    lightning_file = fire_gis_dir / lightning_file
    burnprob_file = fire_gis_dir / burnprob_file
    total_fire_risk = quantify_fire_risk(power_model_risk, fire_gis_dir, soil_file, soil_moisture_file, veg_file, lightning_file, burnprob_file, temperature_file, wind_file)
    print(total_fire_risk)

