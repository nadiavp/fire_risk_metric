import opendssdirect as odd
import sys
# this script runs through the opendss files of a power system model
# and applies the fire risk metric which quantifies the risk of the
# power system igniting a fire

def get_dist_fire_traits(power_model, dist_trait_names):
    dist_fire_components = []
    dist_risk_score = {}
    return dist_risk_score, dist_fire_components

def get_dist_fire_locations(dist_fire_components, power_model):
    risk_component_coords = {}
    return risk_component_coords

def get_terrain_coords(fire_gis_dir):
    terrain_component_coords = {}
    return terrain_component_coords

def distribution_terrain_risk_matrix():
    dist_trait_names = ['line_to_veg_dist', 'line_to_line_dist', 'line_to_gnd_dist', 'line_age',
                        'transformer_age', 'oil_type_transformer', 'overhead', 'uninsulated',
                        'line_peak_load', 'transformer_peak_load', 'hif_detection', 'powersafety_shutoff',
                        'misting_fire_suppresion', 'response_team_coordination', 'high_fidelity_tracking']
    terrain_trait_names = ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']
    dist_terr_cross_list = [['ground_veg', 'veg_moisture', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp'],
                            ['ground_veg', 'soil_saturation', 'veg_moisture', 'lightning', 'wind_speed', 'ambient_temp']]
    return dist_trait_names, terrain_trait_names, dist_terr_cross_list

def quantify_fire_risk(power_model_dir, fire_gis_dir):
    # start with a fire risk of zero and slowly add to that
    total_fire_risk = 0
    # initialize list of terrain traits
    # establish fire risk correlations
    dist_trait_names, terrain_trait_names, dist_terr_cross_list = distribution_terrain_risk_matrix()
    # load the power model
    power_model = odd.run_command(power_model_dir)
    # get power model traits according to metric
    dist_trait_score, dist_fire_components = get_dist_fire_traits(power_model, dist_trait_names)
    # get locations for power system components and assign locations to fire risk traits
    power_component_coords = get_dist_fire_locations(dist_fire_components, power_model)
    # get locations for fire susceptible attributes
    terrain_component_coords = get_terrain_coords(fire_gis_dir)
    # if grid fire risk traits and landscape/climate fire risk traits are within
    # __ meters of each other, consider them co-located and multiply
    dist_index = 0
    for ps_coord in power_component_coords:
        terrain_index = 0
        for tc_coord in terrain_component_coords:
            if (terrain_trait_names[terrain_index] in dist_terr_cross_list[dist_index]) and abs(ps_coord-tc_coord) <= 6:
                total_fire_risk += dist_trait_score[dist_index] * terrain_trait_score[terrain_index]
            terrain_index += 1
        dist_index += 1

    return total_fire_risk

if "__name__"==__main__:
    power_model_dir = sys.argv[1]
    fire_gis_dir = sys.argv[2]
    total_fire_risk = quantify_fire_risk(power_model_dir, fire_gis_dir)
    print(total_fire_risk)
