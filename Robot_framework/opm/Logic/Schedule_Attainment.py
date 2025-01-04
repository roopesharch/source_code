def calculate_ytd_and_cmonth_scheduleattainment(data,selected_date):
    site_platform_mapping = {
        "Athens_API-S": "SM",
        "Beerse_FF": "SM",
        "Gurabo_FF": "SM",
        "Latina_FF": "SM",
        "Geel_API-S": "SM",
        "Puebla_FF": "SM",
        "Xian": "SM",
        "Fuji_FF": "SM",
        "Cork_API-S": "SM",
        "Cork_API-L": "LM",
        "Schaffhausen_API-S": "SM",
        "Schaffhausen_FF": "LM",
        "Incheon_FF": "LM",
        "Leiden_API-L": "LM",
        "Malvern_API-L": "LM",
        "Manati_API-L": "LM",
        "SaoJose_FF": "SM",
        "Xian2_FF": "SM",
    }
    data['STDRPTGSITENM'] = data['STDRPTGSITENM'].str.strip().str.replace(r'\s+', ' ', regex=True)
    data['Platform'] = data['STDRPTGSITENM'].map(site_platform_mapping)
    cmonth_data = data[data["FISCMOYRNM"] == selected_date]
   
    # Calculate totals and counts for each group for YTD
    ytd_totals = data.groupby("Platform")["SCHEDATTNMNT"].sum()
    ytd_counts = data.groupby("Platform")["SCHEDATTNMNT"].count()
    ytd_percentages = (ytd_totals / ytd_counts * 100).round(1).fillna(0)
   
    # Calculate totals and counts for each group for CMonth
    cmonth_totals = cmonth_data.groupby("Platform")["SCHEDATTNMNT"].sum()
    cmonth_counts = cmonth_data.groupby("Platform")["SCHEDATTNMNT"].count()
    cmonth_percentages = (cmonth_totals / cmonth_counts * 100).round(1).fillna(0)
 
    # Convert to the desired dictionary format
    result_dict = {
        platform: [f"{ytd_percentages.get(platform, 0):.1f}%", f"{cmonth_percentages.get(platform, 0):.1f}%"]
        for platform in ytd_percentages.index.union(cmonth_percentages.index)
    }
   
    return  result_dict
