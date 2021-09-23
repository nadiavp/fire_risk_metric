from concurrent.futures import ProcessPoolExecutor, as_completed, wait
import multiprocessing
import os
import pandas as pd

data_location = os.path.join('/datasets','SMART-DS','v1.0','2016','SFO')

def run_analysis(region):
        metrics_location = os.path.join(data_location,region,'scenarios','solar_medium_batteries_low_timeseries','metrics.csv')
        metrics = pd.read_csv(metrics_location,header=0)

        overhead_map = {}
        line_overload_map = {}
        transformer_overload_map = {}

        for idx,row in metrics.iterrows():
            feeder = row['Feeder Name']
            substation = row['Substation Name']
            if feeder == 'subtransmission':
                continue
            underground_percent = row['Overhead Percentage of Medium Voltage Line Miles']
            number_transformers = row['Number of Transformers']
            total_line_length = row['Medium Voltage Length (miles)']
            transformer_overload_data = pd.read_csv(os.path.join(data_location,region,'scenarios','base_timeseries','opendss',substation,feeder,'analysis','peak_transformer_overloads.csv'),header=0)
            if number_transformers == 0:
                transformer_overload_ratio = 0
            else:
                transformer_overload_ratio = len(transformer_overload_data)/number_transformers
            line_overload_data = pd.read_csv(os.path.join(data_location,region,'scenarios','base_timeseries','opendss',substation,feeder,'analysis','peak_line_overloads.csv'),header=0)
            all_lines = set()
            for idx,row in line_overload_data.iterrows():
                all_lines.add(row['Name'])
           
            total_overload_miles = 0
            with open(os.path.join(data_location,region,'scenarios','base_timeseries','opendss',substation,feeder,'Lines.dss'),'r') as lines_file:
                for row in lines_file.readlines():
                    sp = row.split()
                    overloaded = False
                    for token in sp:
                        if token.startswith('Line.'):
                            if token in all_lines:
                                overloaded = True
                        if token.startswith('Length='):
                            length = token.split('=')[1]
                            length = float(length)*0.621371
                            total_overload_miles+=length
            line_overload_ratio = total_overload_miles/total_line_length
            overhead_map[feeder] = 100-underground_percent
            line_overload_map[feeder] = line_overload_ratio
            transformer_overload_map[feeder] = transformer_overload_ratio
        return (overhead_map,transformer_overload_map,line_overload_map)


all_regions = []
for region in  os.listdir(data_location):
    if region.startswith('P'):
        all_regions.append(region)

nprocs = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=nprocs)
result = pool.map(run_analysis, all_regions) #Format is (overhead_map, transformer_overload_map, line_overload_map)
pool.close()
pool.join()

all_overhead_map = {}
all_transformer_overload_map = {}
all_line_overload_map = {}

for overhead_map, transformer_overload_map,line_overload_map in result:
    all_overhead_map.update(overhead_map)
    all_transformer_overload_map.update(transformer_overload_map)
    all_line_overload_map.update(line_overload_map)


result = pd.DataFrame(columns=['feeder','percentage overhead','percentage transformers overloaded','percentage line length overloaded','risk factor'])
cnt = 0
for feeder in all_overhead_map:
    risk_factor = all_overhead_map[feeder]+all_transformer_overload_map[feeder]+all_line_overload_map[feeder]
    result.loc[cnt] = (feeder,all_overhead_map[feeder],all_transformer_overload_map[feeder],all_line_overload_map[feeder],risk_factor)
    cnt+=1
#result = result.sort_values(by=['risk factor','feeder'],ascending=False)
result.to_csv('smbl_summary.csv',header=True,index=False)

