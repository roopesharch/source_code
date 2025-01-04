import pandas as pd
from datetime import datetime
import os
import sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/'))
# Add the parent directory to the system path
sys.path.append(PARENT_DIR)

def calculate_ytd_cmonth_batch_std(data,selected_date):
    file_path = os.path.join(PARENT_DIR, 'mapping_file', 'Batch_std.csv')
    df1 = pd.read_csv(file_path)
    brand_to_platform = dict(zip(df1["Brand"].str.lower(), df1["SM_LM"]))
    data['Platform'] = data['Brand'].str.lower().map(brand_to_platform)
    site_mapping = {
    "Incheon_FF": "Incheon",
    "Cork_API-L": "Cork Bio",
    "Gurabo_FF": "Gurabo Parenteral",
    "Leiden_API-L": "Leiden",
    "Schaffhausen_FF": "Schaffhausen FF",
    "Malvern_API-L": "Malvern"
    }
    filtered_Data = data[(data["Platform"].str.upper() == 'LM') & (data["Reporting Site"].isin(site_mapping.keys()))]
    filtered_Data["Reporting Site"] = filtered_Data["Reporting Site"].replace(site_mapping)
    # Calculate totals and counts for each group for YTD
    ytd_totals = filtered_Data[filtered_Data["Produced At least 90% Scheduled Quantity"] == "Attained"].groupby("Reporting Site").size()
    ytd_counts = filtered_Data.groupby("Reporting Site")["Produced At least 90% Scheduled Quantity"].count()
    ytd_percentages = (ytd_totals / ytd_counts * 100).round(1).fillna(0)

    #Filter data for the current month (CMonth)
    cmonth_data = filtered_Data[(filtered_Data["Reporting Month"] == int(selected_date))]

    # Calculate totals and counts for each group for CMonth
    cmonth_totals = cmonth_data[cmonth_data["Produced At least 90% Scheduled Quantity"] == "Attained"].groupby("Reporting Site").size()
    cmonth_counts = cmonth_data.groupby("Reporting Site")["Produced At least 90% Scheduled Quantity"].count()
    cmonth_percentages = (cmonth_totals / cmonth_counts * 100).round(1).fillna(0)

    # Convert to dictionary format
    result_dict = {
        site: [
           f"{ytd_percentages.get(site, 0):.1f}%",
           f"{cmonth_percentages.get(site, 0):.1f}%"
       ]
        for site in ytd_percentages.index.union(cmonth_percentages.index)
    }

    return result_dict
