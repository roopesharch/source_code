import pyodbc
import time
import opm_rb.core.setup as stp
from robot.libraries.BuiltIn import BuiltIn


## Importing the main library used to connect to Denodo via JDBC
import jaydebeapi as dbdriver
from socket import gethostname
import opm_rb.core.setup as stp

import warnings
warnings.filterwarnings('ignore')
from robot.libraries.BuiltIn import BuiltIn
import pyodbc
import pandas as pd
from pandas import DataFrame



def Read_cv_Synapse_stockout_Data(final_data_dict):

    start_time = time.time()

    # logger.info("Reading Stockouts CV data")
    cursor = stp.connect_opm_azure_database()
    print("Reading synapse CV data from database")
    location_list = []
    for i in final_data_dict:
        location_list.append(str(i))

    # location_list = ['APAC', 'WW', 'NA', 'EMEA', 'LATAM']
    # year_month = conf.read_conf('opm', 'year_month')
    # year_month ='202408'
    year_month = stp.get_current_date('yearmonth')
    year = stp.get_current_date('year')
    # year = '2024'

    # cycle_month_snapshot = '2024-08'
    cycle_month_snapshot = stp.get_current_date('year-month')


    # Get the Actual YTD details from synapse database for stockouts in this for loop for all the locations in location list and append output value to final dictionary
    for i in location_list:
        if i != 'WW':
            ydt_query = "select count(*) from pes.stock_outs_history where original_record=1  and upper(asd_reporting_indicator) in ('Y','N/A') and upper(stockout_reason_code) like '%TRANSPORT%' and upper(affiliate_or_supply_driven) = 'SUPPLY DRIVEN'	  and region='" + i + "'and cycle_month_snapshot='"+cycle_month_snapshot+"'  and   fiscal_year_number ='" + year + "'"
        else:
            ydt_query = "select count(*) from pes.stock_outs_history where original_record=1  and upper(asd_reporting_indicator) in ('Y','N/A') and upper(stockout_reason_code) like '%TRANSPORT%' and upper(affiliate_or_supply_driven) = 'SUPPLY DRIVEN'	and cycle_month_snapshot='" + cycle_month_snapshot + "'  and  fiscal_year_number ='" + year + "'"

        time.sleep(4)
        cursor.execute(ydt_query)
        for j in cursor:
            # print("++" + str(i))
            # print(j[0])
            output_val = int(j[0])
            try:
                final_data_dict[i].append(output_val)
            except:
                temp_ls=[]
                final_data_dict[i]=temp_ls
                final_data_dict[i].append(output_val)
            break
        time.sleep(4)

    # Get the monthly actual details from synapse database for stockouts in this for loop for all the locations in location list and append output value to final dictionary
    for i in location_list:
        if i != 'WW':
            mtd_query = "select count(*) from pes.stock_outs_history where original_record=1  and upper(asd_reporting_indicator) in ('Y','N/A') and upper(stockout_reason_code) like '%TRANSPORT%' and upper(affiliate_or_supply_driven) = 'SUPPLY DRIVEN'	  and region='" + i + "'and cycle_month_snapshot='" + cycle_month_snapshot + "'  and   fiscal_year_month_number ='" + year_month + "'"

        else:
            mtd_query = "select count(*) from pes.stock_outs_history where original_record=1  and upper(asd_reporting_indicator) in ('Y','N/A') and upper(stockout_reason_code) like '%TRANSPORT%' and upper(affiliate_or_supply_driven) = 'SUPPLY DRIVEN'	and cycle_month_snapshot='" + cycle_month_snapshot + "'  and   fiscal_year_month_number ='" + year_month + "'"

        cursor.execute(mtd_query)
        for j in cursor:
            print("++" + str(i))
            print(j[0])
            output_val = int(j[0])
            # final_data_dict[i].append(output_val)
            try:
                final_data_dict[i].append(output_val)
            except:
                temp_ls=[]
                final_data_dict[i]=temp_ls
                final_data_dict[i].append(output_val)
            break
    # self.final_data_dict = final_data_dict
    cursor.close()
    print("Successfully Read Synapse CV data")
    print(final_data_dict)
    print("Time taken for on complete run --- %s seconds ---" % (time.time() - start_time))
    return final_data_dict



# final_data_dict = {'APAC': [0, 0], 'WW': [3, 0], 'NA': [0, 0], 'EMEA': [0, 0], 'LATAM': [3, 0]}
#
# md=Read_cv_Synapse_stockout_Data(final_data_dict)
# print(md)






cursor = stp.connect_denodo()

## Define a cursor and execute' the results


def get_site_id(region):
    cursor = stp.connect_QA_synapse_database()

    if region != 'WW':
        query = ''' select distinct Parent_ehsssubdivid from deliver_site_heirachy_mapping where "Deliver Site Cluster" like '%'''+region+'''%' or alsoknownas like '%'''+region+'''%' or "Deliver Region" like '%'''+region+'''%' '''
    else:
        query = ''' select distinct Parent_ehsssubdivid from deliver_site_heirachy_mapping'''

    # print(query)
    df = pd.read_sql(query, cursor)
    df.fillna('0000', inplace=True)
    # print(df)
    region_list=['0000-0000','0000-0000']
    for i in df:
        for j in range(len(df)):
            # print(df[i][j])
            region_list.append(str(df[i][j]))
    return region_list

# property_code = get_property_code('Japan')
# print(property_code)

def get_site_id_and_propertycode(region):
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

a=get_site_id_and_propertycode('EMEA')
print(a[0])
print(a[1])


def SIF(site_id_list, property_code):
    year = stp.get_current_date('year')
    year_month = stp.get_current_date('year-month')
    start_date = str(year)+'-01-01'
    end_date = str(year_month)+'-02'
    cur = stp.connect_denodo()

    # if len(ls) == 0:
    #     ls=['0000']

    monthly = ''' select "Number of SIF" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  like ''' +"'"+str(year_month)+"%'"+ ''' and "Parent_ehsssubdivid"  in  '''+str(tuple(site_id_list)) +''' and  propertycode in '''+ str(tuple(property_code))
    # print(monthly)

    cur.execute(monthly)
    df = DataFrame(cur.fetchall())
    # df.fillna('0000', inplace=True)
    # print(df)

    Number_of_SIF = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIF +=int(df[0][i])

    # print('C_Mon of ')
    # print(Number_of_SIF)

    ytd = ''' select "Number of SIF (YTD)" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  between '''+"'"+start_date+"'"+''' and '''+"'"+end_date+"'"+''' and "Parent_ehsssubdivid"  in  ''' + str(tuple(site_id_list))+''' and  propertycode in '''+ str(tuple(property_code))
    # cur = stp.connect_denodo()
    # print(ytd)
    cur.execute(ytd)
    df = DataFrame(cur.fetchall())
    # df.fillna('0000', inplace=True)
    # print(df)

    Number_of_SIF_YTD = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIF_YTD += int(df[0][i])

    # print('YTD of ')
    # print(Number_of_SIF_YTD)
    return [Number_of_SIF_YTD, Number_of_SIF]

# la = SIF([5568, '5485'])
# la = SIF(property_code)
# la = SIF(['0000-0000', '0000-0000', '76236 - 10278', '15396 - 10304'], ['0000', '0000', '7642', '5042'])
# print(la)


def SIF_P(site_id_list, property_code):
    year = stp.get_current_date('year')
    year_month = stp.get_current_date('year-month')
    start_date = str(year) + '-01-01'
    end_date = str(year_month) + '-02'
    cur = stp.connect_denodo()

    ytd = ''' select "Number of SIFp w Highest Possible Control (YTD)", "Number of SIF-p Reported (YTD)" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  between '''+"'"+start_date+"'"+''' and '''+"'"+end_date+"'"+''' and "Parent_ehsssubdivid"  in  ''' + str(tuple(site_id_list))+''' and  propertycode in '''+ str(tuple(property_code))
    # print(ytd)
    cur.execute(ytd)
    df = DataFrame(cur.fetchall())

    Number_of_SIFp_w_Highest_Possible_Control_YTD = 0
    Number_of_SIFp_Reported_YTD = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIFp_w_Highest_Possible_Control_YTD +=int(df[0][i])
        Number_of_SIFp_Reported_YTD +=int(df[1][i])

    # print('YTD of ')
    # print(Number_of_SIFp_w_Highest_Possible_Control_YTD, Number_of_SIFp_Reported_YTD)
    if Number_of_SIFp_Reported_YTD != 0:
        ytd = ((Number_of_SIFp_w_Highest_Possible_Control_YTD/Number_of_SIFp_Reported_YTD)*100)
        # print((Number_of_SIFp_w_Highest_Possible_Control_YTD/Number_of_SIFp_Reported_YTD))
    elif Number_of_SIFp_w_Highest_Possible_Control_YTD == 0 and Number_of_SIFp_Reported_YTD == 0:
        ytd = 100
    else:
        ytd = Number_of_SIFp_Reported_YTD*100
        # print(Number_of_SIFp_Reported_YTD)

    month = ''' select "Number of SIFp with Highest Possible Control", "Number of SIFp Reported" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  like ''' + "'" + str(year_month) + "%'" + ''' and "Parent_ehsssubdivid"  in  ''' + str(tuple(site_id_list))+''' and  propertycode in '''+ str(tuple(property_code))
    # print(month)
    cur.execute(month)
    df = DataFrame(cur.fetchall())

    Number_of_SIFp_w_Highest_Possible_Control = 0
    Number_of_SIFp_Reported = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIFp_w_Highest_Possible_Control += int(df[0][i])
        Number_of_SIFp_Reported += int(df[1][i])

    # print('YTD of ')
    # print(Number_of_SIFp_w_Highest_Possible_Control, Number_of_SIFp_Reported)
    if Number_of_SIFp_Reported != 0:
        monthly = ((Number_of_SIFp_w_Highest_Possible_Control / Number_of_SIFp_Reported)*100)
        # print((Number_of_SIFp_w_Highest_Possible_Control / Number_of_SIFp_Reported))
    elif Number_of_SIFp_w_Highest_Possible_Control == 0 and Number_of_SIFp_Reported == 0:
        monthly = 100
    else:
        monthly = Number_of_SIFp_Reported*100
        # print(Number_of_SIFp_Reported)

    return [ytd, monthly]

# la = SIF_P([5568, '5485'])
# print(la)
# la = SIF_P(property_code)
# print(la)
# la = SIF_P(['0000-0000', '0000-0000', '76236 - 10278', '15396 - 10304'], ['0000', '0000', '7642', '5042'])
# print(la)


def SIF_P_R(ls,property_code):
    year = stp.get_current_date('year')
    year_month = stp.get_current_date('year-month')
    start_date = str(year) + '-01-01'
    end_date = str(year_month) + '-02'
    cur = stp.connect_denodo()

    ytd = ''' select "Number of SIFp w Highest Possible Control (YTD)", "Number of SIF-p Reported (YTD)" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  between '''+"'"+start_date+"'"+''' and '''+"'"+end_date+"'"+''' and "Parent_ehsssubdivid"  in  ''' + str(tuple(ls))+''' and  propertycode in '''+ str(tuple(property_code))

    cur.execute(ytd)
    df = DataFrame(cur.fetchall())

    Number_of_SIFp_w_Highest_Possible_Control_YTD = 0
    Number_of_SIFp_Reported_YTD = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIFp_w_Highest_Possible_Control_YTD += int(df[0][i])
        Number_of_SIFp_Reported_YTD += int(df[1][i])

    # print('YTD of ')
    # print(Number_of_SIFp_w_Highest_Possible_Control_YTD, Number_of_SIFp_Reported_YTD)
    if Number_of_SIFp_Reported_YTD != 0:
        ytd = (Number_of_SIFp_w_Highest_Possible_Control_YTD / Number_of_SIFp_Reported_YTD)
        # print((Number_of_SIFp_w_Highest_Possible_Control_YTD/Number_of_SIFp_Reported_YTD))
    else:
        ytd = 0
        # ytd = Number_of_SIFp_Reported_YTD
        # print(Number_of_SIFp_Reported_YTD)

    month = ''' select "Number of SIFp with Highest Possible Control", "Number of SIFp Reported" from "Enterprise Dashboard Summary Metrics"  where   "Reporting Period"  like ''' + "'" + str(year_month) + "%'" + ''' and "Parent_ehsssubdivid"  in  ''' + str(tuple(ls))+''' and  propertycode in '''+ str(tuple(property_code))

    cur.execute(month)
    df = DataFrame(cur.fetchall())

    Number_of_SIFp_w_Highest_Possible_Control = 0
    Number_of_SIFp_Reported = 0
    for i in range(len(df)):
        # print(df[0][i],df[1][i])
        Number_of_SIFp_w_Highest_Possible_Control += int(df[0][i])
        Number_of_SIFp_Reported += int(df[1][i])

    # print('YTD of ')
    # print(Number_of_SIFp_w_Highest_Possible_Control, Number_of_SIFp_Reported)
    if Number_of_SIFp_Reported != 0:
        monthly = (Number_of_SIFp_w_Highest_Possible_Control / Number_of_SIFp_Reported)
        # print((Number_of_SIFp_w_Highest_Possible_Control / Number_of_SIFp_Reported))
    else:
        monthly = 0
        # monthly = Number_of_SIFp_Reported
        # print(Number_of_SIFp_Reported)

    return [ytd, monthly]

# la = SIF_P_R([5568, '5485'])
# print(la)
# la = SIF_P_R(property_code)
# print(la)

# la = SIF_P_R(['0000-0000', '0000-0000', '76236 - 10278', '15396 - 10304'], ['0000', '0000', '7642', '5042', 'TBC'])
# print(la)




def get_ehs_denodo_data(dic):
    for i in dic:
        if isinstance(dic[i], list):
            dic[i].append('No Data')
            dic[i].append('No Data')
        else:
            for j in dic[i]:
                result_list = get_site_id_and_propertycode(str(j))
                site_ids= result_list[0]
                property_code_list= result_list[1]

                # print(str(j))
                # print(site_ids)
                # print(property_code_list)
                if "Severe Injury or Fatality (SIF)" in str(i):
                    la = SIF(site_ids, property_code_list)
                    for k in la:
                        dic[i][j].append(k)

                elif "Severe Injury or Fatality - Precursor (SIF-P) Reporting" in str(i):
                    la = SIF_P_R(site_ids, property_code_list)
                    for k in la:
                        dic[i][j].append(k)
                elif "Severe Injury or Fatality - Controls (SIF-P Controls)" in str(i):
                    la = SIF_P(site_ids, property_code_list)
                    for k in la:
                        dic[i][j].append(k)
    # from datetime import datetime
    #
    # BuiltIn().log_to_console(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # BuiltIn().log_to_console(dic)
    return dic

#
# dics = {"WW:Severe Injury or Fatality (SIF)": ['No Data', 'No Data'],
#         "WW:Severe Injury or Fatality - Precursor (SIF-P) Reporting": {'WW': ['N/A', 'N/A'], 'NA': ['N/A', 'N/A'], 'LATAM': ['N/A', 'N/A'], 'EMEA': ['N/A', 'N/A'], 'APAC': ['N/A', 'N/A']},
#         "WW:Severe Injury or Fatality - Controls (SIF-P Controls)": {'WW': ['N/A', 'N/A'], 'NA': ['N/A', 'N/A'], 'LATAM': ['N/A', 'N/A'], 'EMEA': ['N/A', 'N/A'], 'APAC': ['N/A', 'N/A']}}
# dics = get_ehs_denodo_data(dics)
# print(dics)


def get_sub_regions_from_region_via_database(region_type, region_lst):

    region_heirachy={'region': ["Deliver Region"], 'country': ["Deliver Site Cluster"]}
    cursor = stp.connect_QA_synapse_database()
    region_lst = tuple(region_lst)

    query = ''' select  Distinct "''' + region_heirachy[region_type][0] + '''" , Parent_ehsssubdivid from	  deliver_site_heirachy_mapping where "''' + region_heirachy[region_type][0] + '''" in '''+ str(region_lst)

    print(query)
    df = pd.read_sql(query, cursor)
    print(df)
    # for i in df:
    #     for j in range(len(df)):
    #         print(df[i][j])

# region_list=['LATAM']
# get_sub_regions_from_region_via_database('country', region_list)

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





