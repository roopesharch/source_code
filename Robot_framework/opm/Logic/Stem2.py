import pandas as pd
from datetime import datetime
import os
import sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
# Add the parent directory to the system path
sys.path.append(PARENT_DIR)


def calculate_ytd_and_cmonth_stem(data, slected_date):
    file_path = os.path.join(PARENT_DIR, 'mapping_file', 'stem.csv')
    df1 = pd.read_csv(file_path)
    brand_to_platform = dict(zip(df1["Brand"].str.lower(), df1["Platform"]))
    data['Platform'] = data['International Brand'].str.lower().map(brand_to_platform)
    filtered_data = data[(data["Platform"].isin(["Large Molecule", "Small Molecule"]))]
    cmonth_data = filtered_data[(filtered_data["Fiscal Mth Yr Nm"] == slected_date)]
    ytd_count = len(filtered_data["Issue#"].unique())
    month_count = len(cmonth_data["Issue#"].unique())
    result_dict = {'LM & SM': [ytd_count, month_count]}
    return  result_dict