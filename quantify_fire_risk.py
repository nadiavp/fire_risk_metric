import opendssdirect as odd
import sys
import pathlib
import pyproj
import json
import pandas as pd
from scipy.io import netcdf
import netCDF4
import gzip
from sklearn.neighbors import BallTree
# this script runs through the opendss files of a power system model
# and applies the fire risk metric which quantifies the risk of the
# power system igniting a fire
dist_trait_names = ['line_to_veg_dist', 'line_to_line_dist', 'line_to_gnd_dist', 'line_age',
                    'transformer_age', 'oil_type_transformer', 'overhead', 'uninsulated',
                    'line_peak_load', 'transformer_peak_load', 'hif_detection', 'powersafety_shutoff',
                    'misting_fire_suppresion', 'response_team_coordination', 'high_fidelity_tracking']

terrain_trait_names = ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']

def get_dist_fire_traits(dist_risk_components, bus_coords):
    # check the risks we know just by looking at the distribution model 
    # default to 0
    n_traits = len(dist_trait_names)
    #dist_fire_components = []
    dist_risk_scores = {}
    # add risks for oil type transformer, uninsulated, hif, psps, misting, coordination, and tracking
    for dfc in dist_risk_components:
        dist_risk_scores[dfc['feeder']] = {}
        risk_score = [0]*n_traits
        overhead = False
        # check age
        if dfc:
            risk_score[3] = 0
        #check if overhead, denoted in line code. OH for high voltage or non-inner city lines
        percent_overhead = dfc['percentage overhead']
        risk_score[6] = percent_overhead/10
        # check if insulated. True for low voltage in city or UG lines
        if 'lv' not in dfc['feeder']:
            uninsulated = percent_overhead
            risk_score[7] = uninsulated/10
        # check line peak current
        risk_score[8] = dfc['percentage line length overloaded']
        # check transformer loading
        risk_score[9] = dfc['percentage transformers overloaded']
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
        risk_score[11] = 4 # power safety shutoffs used in this area
        risk_score[12] = 10 # misting fire hose nozzle
        risk_score[13] = 4 # response team coordination
        risk_score[14] = 1 # high fidelity tracking
        # add risk score to json
        dist_fire_components[dfc['feeder']]['risks'] = risk_score
        dist_fire_components[dfc['feeder']]['coords'] = bus_coords[dfc['feeder']]
    # export distribution risk score to json format
    with open('dist_risk_scores.json', 'w') as dist_score_file:
        json.dump(dist_fire_components, dist_score_file)
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
    sub_regions = []
    potential_sub_regions = power_model_dir.glob('*')
    for folder in potential_sub_regions:
        sub_region_name = str(folder).split('/')[-1]
        if sub_region_name.startswith(region_name) and (folder / "DSSfiles/Buscoords.dss").exists() and (folder / "DSSfiles/Transformers.dss").exists():
            sub_regions.append(folder)
            trans_file = (sub_region / "DSSfiles/Transformers.dss")
            line_file = (sub_region / "DSSfiles/Lines.dss")
            bus_file = (sub_region / "DSSfiles/Buscoords.dss")
            linecode_file = (sub_region / "DSSfiles/LineCodes.dss")
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
                    if len(line)>4:
                        line_entry = line.split(' ')
                        name = line_entry[1]
                        bus1 = line_entry[3].replace('bus1=', '').split('.')[0]
                        bus2 = line_entry[4].replace('bus2=', '').split('.')[0]
                        linecode = line_entry[-1].replace('Linecode=', '')
                        # add info to dictionary
                        dist_fire_components[name] = {}
                        dist_fire_components[name]['coords'] = [bus_coords[bus1]]
                        dist_fire_components[name]['coords'].append(bus_coords[bus2])
                        dist_fire_components[name]['normamps'] = line_codes[linecode]
            # read in transformers
            with open(trans_file, 'r') as tf:
                tf_lines = tf.readlines()
                for line in tf_lines:
                    if len(line)>4:
                        line_entry = line.split(' ')
                        name = line_entry[1]
                        bus = line_entry[8].replace('bus=', '')
                        # add info to dictionary
                        dist_fire_components[name] = {}
                        dist_fire_components[name]['coords'] = [bus_coords[bus]]
                        dist_fire_components[name]['normamps'] = 1000000000 # need to find a way to add rating
    return dist_fire_components, bus_coords

def get_terrain_coords(soil_file, veg_file, lightning_file, burnprob_file):
    # parse the files and select only the bay area: as defined as
    # north to Sacremento: 38.5816N 121.4944W, 
    # south and east to Fresno: 36.7378N 119.7871W, 
    # and west to longitude 120W
    terrain_component_coords = {}
    terrain_component_coords["type"] = "FeatureCollection"
    terrain_component_coords["features"] = []

    # get table for soil saturation
    soil_table = pd.read_csv(soil_file)
    soil_table = soil_table[['Region', 'Latitude', 'Longitude', 'Species', 'Soil_type', 'Soil_drainage',
                            'Ecosystem_type', 'Ecosystem_state', 'Leaf_habit']]
    soil_table = soil_table[soil_table['Region']=='California']
    soil_risk_list = []
    for sd, lat, long in zip(soil_table['Soil_drainage'], soil_table['Latitude'], soil_table['Longitude']):#soil_table['Soil_type']):
        # not sure we need anything for soil type
        if sd=='Dry':
            risk = 10
        elif sd=='Wet':
            risk = 0
        else:
            risk=0
        # add rainfall data interrelated with soil drainage to get overall saturation
        #rain_table = pd.read_csv(precip_file)
        
        # add soil risk to geojson
        soil_entry = {}
        soil_entry["type"]="Feature"
        soil_entry["geometry"] = {}
        soil_entry["geometry"]["type"] = "Point"
        soil_entry["geometry"]["coordinates"] = [lat, long]
        soil_entry["properties"]={}
        soil_entry["properties"]["soil_sat"]=risk
        soil_risk_list.append(soil_entry)
        terrain_component_coords["features"].append(soil_entry)
    print('soil saturation risk added to json')
    # get table for lightning
    lightning_risk_list = []
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
    for month in months:
        for lat in latitude:
            if lat>36.7378 and lat<36.7378:
                for long in longitude:
                    if long>119.7871 and long<120:
                        flash = flashes[month, long, lat]
                        risk = flash/0.05 * 10 # normalize with 0.05 and then scale to 10 point scale
                        # add risk to geojson
                        lightning_entry = {}
                        lightning_entry["type"]="Feature"
                        lightning_entry["geometry"] = {}
                        lightning_entry["geometry"]["type"] = "Point"
                        lightning_entry["geometry"]["coordinates"] = [lat, long]
                        lightning_entry["properties"]={}
                        lightning_entry["properties"]["veg_type"]=risk
                        lightning_entry["properties"]["month"]=month
                        lightning_risk_list.append(lightning_entry)
                        terrain_component_coords["features"].append(lightning_entry)
    print('lightning risk added to json')
    # convert ecosystem type and state into fire risk value
    veg_risk_list = []
    vegetation_risk = 0
    for ess, est, lh, lat, long in zip(soil_table['Ecosystem_state'], soil_table['Ecosystem_type'], soil_table['Leaf_habit'], soil_table['Latitude'], soil_table['Longitude']):
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
            # add risk to geojson
            veg_entry = {}
            veg_entry["type"]="Feature"
            veg_entry["geometry"] = {}
            veg_entry["geometry"]["type"] = "Point"
            veg_entry["geometry"]["coordinates"] = [lat, long]
            veg_entry["properties"]={}
            veg_entry["properties"]["veg_type"]=vegetation_risk
            veg_entry["properties"]["month"]=month
            veg_risk_list.append(veg_entry)
            terrain_component_coords["features"].append(veg_entry)
    print('vegetation type risk added to json')
    with open('terrain_risk_scores.json', 'w') as terr_score_file:
        json.dump(terrain_component_coords, terr_score_file)
    print('terrain risk factors recorded in terrain_risk_scores.json')
    return terrain_component_coords, soil_risk_list, lightning_risk_list, veg_risk_list

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
    for dfr in dist_risk:
        dist_coord = dfr['coords']
        all_coords.append(dist_coords)
    
    soil_coords = [sc['coords'] for sc in soil_risk]
    light_coords = [lc['coords'] for lc in lightning_risk]
    veg_coords = [vc['coords'] for vc in veg_risk]
    
    #tree = BallTree(terrain_coords, leaf_size=15)
    #bus_terrain_distances, terrain_indices = tree.query(all_coords, k=1)
    soil_tree = BallTree(soil_coords, leaf_size=10)
    light_tree = BallTree(light_coords, leaf_size=10)
    veg_tree = BallTree(veg_coords, leaf_size=10)
    bus_soil_dist, soil_inds = soil_tree.query(all_coords, k=1)
    bus_light_dist, light_inds = light_tree.query(all_coords, k=1)
    bus_veg_dist, veg_inds = veg_tree.(all_coords, k=1)
    
    # go through nearest neighbor points and assign attributes
    risk_table = distribution_terrain_risk_matrix()
    dist_i = 0
    all_risks = {}
    for dfr in dist_risk:
        feeder_risks = []
        feeder_risks = dfr['risks']
        # ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']
        nearest_soil = soil_risk[soil_inds[dist_i]]
        nearest_light = lightning_risk[light_inds[dist_i]]
        nearest_veg = veg_risk[veg_inds[dist_i]]
        # follow the matrix
        fri=0
        inter_risk = []
        for fr in feeder_risks:
            if 'ground_veg' in risk_table[fri]:
                inter_risk.append(fr*nearest_veg)
            if 'soil_saturation' in risk_table[fri]:
                inter_risk.append(fr*nearest_soil)
            if 'veg_moisture' in risk_table[fri]:
                inter_risk.append(fr*nearest_moist)
            if 'lightning' in risk_table[fri]:
                inter_risk.append(fr*nearest_light)
            #if 'wind_speed' in risk_table[fri]:
            #    inter_risk.append(fr*nearest_wind)
        all_risks[dfr['feeder']]['risk']=sum(inter_risk)
        all_risks[dfr['feeder']]['coords'] = dist_risk['coords']
    return all_risks

def quantify_fire_risk(power_model_dir, fire_gis_dir, soil_file, veg_file, lightning_file, burnprob_file, temperature_file, wind_file):
    # start with a fire risk of zero and slowly add to that
    total_fire_risk = 0
    # initialize list of terrain traits
    # establish fire risk correlations
    dist_terr_cross_list = distribution_terrain_risk_matrix()
    # load the power model
    power_model = odd.run_command(str(power_model_dir / 'DSSfiles/Master.dss'))
    # get locations for power system components and assign locations to fire risk traits
    dist_fire_components, bus_coords = get_dist_fire_locations(power_model_dir)
    # get power model traits according to metric
    dist_fire_components = get_dist_fire_traits(power_model, bus_coords)
    # get locations for fire susceptible attributes
    terrain_component_coords, soil_risk, lightning_risk, veg_risk = get_terrain_coords(soil_file, veg_file, lightning_file, burnprob_file)
    # use nearest neighbor to find interdependent risk factors
    total_fire_risk = get_interrelated_risk(terrain_component_coords, dist_fire_components, soil_risk, lightning_risk, veg_risk, temperature_file, wind_file)
    
    return total_fire_risk

if __name__=="__main__":
    power_model_dir = pathlib.Path("C:/Users/npanossi/Documents/Gemini-XFC/GEMINI-XFC/P1U/solar_medium_batteries_low_timeseries/DSSfiles/p1uhs0_1247")
    #sys.argv[1]
    if isinstance(power_model_dir, str):
        power_model_dir = pathlib.Path(power_model_dir)
    power_model_risk = 'smbl_summary.csv'
    fire_gis_dir = sys.argv[2]
    soil_file = 'srdb-data-V5.csv'
    veg_file = 'cdl_fccs_merge2010/cdl_fccs_merge2010.tif.aux.xml'
    lightning_file = 'VHRMC.nc'
    burnprob_file = 'CONUS_iBP.tif'
    temperature_file = 'temperature.parquet'
    wind_file = 'wind.parquet'
    if isinstance(fire_gis_dir, str):
        fire_gis_dir = pathlib.Path(fire_gis_dir)
        soil_file = fire_gis_dir / soil_file
        veg_file = fire_gis_dir /veg_file
        lightning_file = fire_gis_dir / lightning_file
        burnprob_file = fire_gis_dir / burnprob_file
    total_fire_risk = quantify_fire_risk(power_model_dir, fire_gis_dir, soil_file, veg_file, lightning_file, burnprob_file)
    print(total_fire_risk)
