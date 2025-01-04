# Importing the main library used to connect to Denodo via JDBC
import setup as stp
import warnings
import pandas as pd
from pandas import DataFrame
warnings.filterwarnings('ignore')
cursor = stp.connect_denodo()


# This function is maping for site ID with EHS for future Implementation SO can ignore now
def get_regions_heirarchy_from_opm_db(region_type,region, index):
    region_heirachy={'region': ["Region", "Deliver SubRegion", 'Deliver Cluster', 'Deliver SubCluster', 'Country Name'],
                     'sub_region': ['Deliver SubRegion', 'Deliver Cluster', 'Deliver SubCluster', 'Country Name'],
                     'cluster': ['Deliver Cluste', 'Deliver SubCluster', 'Country Name'],
                     'sub_cluster': ['Deliver SubCluster', 'Country Name']}
    cursor = stp.connect_opm_azure_database()

    query = ''' select distinct ['''+region_heirachy[region_type][index]+'''] from mst.deliver_hierarchy_mapping   where '''+region_heirachy[region_type][0]+" = '"+region+"'"
    print(query)
    df = pd.read_sql(query, cursor)
    df.fillna('none', inplace=True)
    print(df)
    print(len(df))
    region_list= []
    for i in df:
        for j in range(len(df)):
            # print(df[i][j])
            if df[i][j] != 'none' and df[i][j] != '':
                region_list.append(df[i][j])


    print(len(region_list))

    if len(region_list)==0 and len(region_heirachy[region_type])>index:
        index +=1
        region_list = get_regions_heirarchy_from_opm_db(region_type, region, index)

    return region_list

# lst = get_regions_heirarchy_from_opm_db('region','LATAM', 1)
# print(lst)





## Define a cursor and execute' the results


def get_site_id_and_propertycode_for_SIF_Deliver(region):
    cursor = stp.connect_QA_synapse_database()

    if region != 'WW':
        query = ''' select Parent_ehsssubdivid, propertycode from Site_heirarchy where Deliver_Site_Cluster like '%'''+region+'''%' or Delivername like '%'''+region+'''%' or "Deliver_Region" like '%'''+region+'''%' '''
    else:
        query = ''' select Parent_ehsssubdivid, propertycode from Site_heirarchy'''

    # print(query)
    df = pd.read_sql(query, cursor)
    df.fillna('0000', inplace=True)
    # print(df)
    region_list=['0000-0000','0000-0000']
    property_code=['0000', '0000']
    for i in df:
        for j in range(len(df)):
            # print(df[i][j])
            if i == 'Parent_ehsssubdivid' and df[i][j] != 'TBC':
                region_list.append(str(df[i][j]))
            elif i == 'propertycode' and df[i][j] != 'TBC':
                property_code.append(str(df[i][j]))
    return [region_list, property_code]
# print(get_site_id_and_propertycode_for_SIF_Deliver('EMEA'))


def get_denodo_query_result(ytd_query, mtd_query, parent_site_id, property_code):

    year = stp.get_current_date('year')
    year_month = stp.get_current_date('year-month')
    start_date = str(year) + '-01-01'
    end_date = str(year_month) + '-02'
    month_start = str(year_month) + '-01'

    ytd = (((ytd_query.replace('start_date',start_date)).replace('end_date', end_date)).replace('parent_site_id',str(tuple(parent_site_id)))).replace('property_code', str(tuple(property_code)))

    mtd = (((mtd_query.replace('start_date',month_start)).replace('end_date', end_date)).replace('parent_site_id',str(tuple(parent_site_id)))).replace('property_code', str(tuple(property_code)))

    cur = stp.connect_denodo()

    cur.execute(ytd)

    df = DataFrame(cur.fetchall())
    result = []
    result.append(int(df[0][0]) if str(df[0][0]) not in ['None'] else str(df[0][0]))
    try:
        result.append(int(df[1][0]) if str(df[1][0]) not in ['None'] else str(df[1][0]))
    except:
        pass

    cur.execute(mtd)
    df = DataFrame(cur.fetchall())
    result.append(int(df[0][0]) if str(df[0][0]) not in ['None'] else str(df[0][0]))
    try:
        result.append(int(df[1][0]) if str(df[1][0]) not in ['None'] else str(df[1][0]))
    except:
        pass

    return result


# ytd_query= ''' select sum("Number of SIFp w Highest Possible Control (YTD)"), sum("Number of SIF-p Reported (YTD)") from "Enterprise Dashboard Summary Metrics" where "Reporting Period"  between 'start_date' and 'end_date' and "Parent_ehsssubdivid"  in  parent_site_id  and  propertycode in property_code '''
# mtd_query= ''' select sum("Number of SIFp with Highest Possible Control"), sum("Number of SIFp Reported") from "Enterprise Dashboard Summary Metrics" where "Reporting Period"  between 'start_date' and 'end_date' and "Parent_ehsssubdivid"  in  parent_site_id  and  propertycode in property_code '''
# a=get_site_id_and_propertycode_for_SIF_Deliver('EMEA')
# print(a[0])
# print(a[1])
# result = get_denodo_query_result(ytd_query, mtd_query, a[0], a[1])
# print(result)
