import pandas as pd
import glob
import numpy as np

# this script ingests California PUC ignition filings to approximate
# the liklihood of an ignition from different situations based on
# historical data
def read_ignition_reports(iginition_report_dir):
    ignition_reports = glob.glob(ignition_report_dir)
    columns=['Materials at Origin','Land Use at Origin','Fire Size',
            'Fire Suppressed by','Nominal Voltage','Equipment Type Associated to Ignition',
            'Type Of Construction','Outage','Suspected Initiating Event',
            'Equipment Facility Failure','Contact From Object',	'Contributing Factor']
    # categories are only used in some report sheets, if they are used, the column names are slightly different
    info_categories = ['Utility Name','Fire Start','Location', 'Fire', 'Utility Facility','Outage','Field Observations'] 
    cat_columns =['Materials at Origin','Land Use at Origin','Size',
            'Suppressed by','Nominal Voltage','Equipment Type Associated to Ignition',
            'Type','Was There an Outage','Suspected Ignition Cause',
            'Equipment/Facility Failure','Contact From Object',	'Contributing Factor'] 
    # some columns have an alternative name
    alt_columns = ['Material at Origin','Land Use at Origin','Size',
            'Suppressed by','Voltage\n(Volts)','Equipment Involved With Ignition',
            'Type','Was There an Outage','Suspected Initiating Event',
            'Equipment /Facility Failure','Contact From Object',	'Contributing Factor']
    alt2_columns = ['Material at Origin','Land Use at Origin','Size',
            'Suppressed \nby','Voltage','Equipment Involved With Ignition',
            'Type','Was There an Outage','Suspected Initiating Event',
            'Equipment /Facility \nFailure','Contact From \nObject',	'Contributing Factor']
    alt3_columns = ['Material at Origin','Land Use at Origin','Size',
            'Suppressed By','Voltage (KVolts)','Equipment Involved With Ignition',
            'Type','Was There an Outage','Suspected Initiating Event',
            'Equipment/Facility Failure','Contact From Object',	'Contributing Factor']
    alt4_columns = ['Material at Origin','Land Use at Origin','Size',
            'Suppressed by','Voltage\n(Volts)','Equipment Involved With Ignition',
            'Type','Was There an Outage','Suspected Ignition Cause',
            'Equipment /Facility Failure','Contact From Object',	'Contributing Factor']
    alt3_columns = []
    ignition_data = pd.DataFrame(columns)
    for report in ignition_reports:
        print(f'opening:{report}')
        if report.endswith('.csv'):
            df_i = pd.read_csv(report)
            df_i = df_i[columns]
            df_i = df_i.rename(columns={'Materials at Origin': 'Material at Origin',
                                   'Equipment Type Associated to Ignition': 'Equipment Involved With Ignition',
                                   'Type Of Construction': 'Type', 
                                   'Outage': 'Was There an Outage', 
                                   'Suspected Initiating Event': 'Suspected Ignition Cause'})
        elif report.endswith('.xlsx'):
            df_i = pd.read_excel(report, skiprows=1) # the first row will have categories,but we just need the column data
            if 'Nominal Voltage' in df_i.columns:
                df_i = df_i[columns]
                df_i = df_i.rename(columns = {'Equipment Type Associated to Ignition': 'Equipment Involved With Ignition',
                                        'Equipment/Facility Failure': 'Equipment Facility Failure'})
            elif 'Voltage\n(Volts)' in df_i.columns:
                if 'Suspected Ignition Cause' in df_i.columns:
                    df_i = df_i[alt4_columns]
                    df_i = df_i.rename(columns = {'Equipment /Facility Failure': 'Equipment Facility Failure'})
                else:
                    df_i = df_i[alt_columns]
                    df_i = df_i.rename(columns = {'Suspected Initiating Event': 'Suspected Ignition Cause',
                        'Equipment /Facility Failure': 'Equipment Facility Failure'})
            elif 'Voltage' in df_i.columns:
                df_i = df_i[alt2_columns]
                df_i = df_i.rename(columns = {'Suspected Initiating Event': 'Suspected Ignition Cause',
                                       'Equipment /Facility \nFailure': 'Equipment Facility Failure',
                                       'Contact From \nObject': 'Contact From Object'
                                       })
            else: #'Voltage (KVolts)' in df_i.columns:
                df_i = df_i[alt3_columns]
                df_i = df_i.rename(columns = {'Suspected Initiating Event': 'Suspected Ignition Cause',
                                       'Equipment/Facility Failure': 'Equipment Facility Failure'})
        ignition_data = pd.concat([ignition_data, df_i], ignore_index=True)
    ignition_data.to_csv('all_iou_ignition_records.csv', index=False)

    return ignition_data

def generate_liklihood_matrix(ignition_data, matrix_struct):
    # the structure of the liklihood matrix follows the same structure as the matrix in quantify_fire_risk
    # in the liklihood_matrix each entry corresponds to the liklihood of that ignition type occuring based
    # on historical ignition records
    # The liklihood_matrix is used in quantify_fire_risk and element-wise multiplied by the fire risk matrix to get an overall score
    n_ign = len(ignition_data) # total number of reported ignitions
    liklihood_matrix = np.zeros(len(matrix_struct))
    i = 0
    for entry in matrix_struct:
        liklihood_matrix[i] = len(ignition_data['']=='')/n_ign
        i+=1

    return liklihood_matrix

if __name__ == "__main__":
    ignition_report_dir = "ignition_reports/*"
    ignition_data = read_ignition_reports(ignition_report_dir)
    print(ignition_data)
    liklihood_matrix = generate_liklihood_matrix(ignition_data)
