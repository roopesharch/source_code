from source import read_config as conf
from source import selenium_functions as sel
from source import setep as stp
import time
import pandas as pd
from datetime import datetime
import sys


def extract_otif_d_data(driver, otif_title, prefix, lst, final_data_dict):
    prefix = prefix.strip()
    xpath = "//*[text()='"+otif_title+"']/../.."
    [element, result] = sel.check_presence_of_element(driver, xpath, 80)
    if not result: raise
    x = element.text.split("\n")
    for i in lst:
        if i in x:
            y = x.index(i)
            if prefix == 'avoid':
                final_data_dict[i] = [str(x[y+2]), str(x[y+3])]
            else:
                final_data_dict[prefix+'_'+i] = [str(x[y+2]), str(x[y+3])]
#             print(x[y+2])
#             print(x[y+3])
    return [driver, final_data_dict]


class Read_Kelvin_otif_d_Data:
    def __init__(self, final_data_dict):

        # set the driver ready
        driver = stp.connect_webdriver()
        driver = stp.kelvin_login(driver,  conf.read_conf('kelvin', 'Deliver_score_page'))

        otif_title = ["OTIF-D", "OTIF-D FM WW", "OTIF-D FM NA", "OTIF-D FM EMEA", "OTIF-D FM LATAM", "OTIF-D FM APAC"]
        region = ['WW', 'NA', 'EMEA', 'LATAM', 'APAC']
        mtric_l2 = ['Customer', 'Order Mgmt', 'Prod Avail', 'Distribution', 'Transportation']
        for i in otif_title:

            if i == "OTIF-D":
                [driver, final_data_dict] = extract_otif_d_data(driver, i, 'avoid', region, final_data_dict)

            else:
                t = str(i).rsplit(' ', 1)
                [driver, final_data_dict] = extract_otif_d_data(driver, i, t[len(t)-1].strip()+" ", mtric_l2, final_data_dict)

        self.final_data_dict = final_data_dict
        self.driver = driver
        print(final_data_dict)
# final_data_dict = {}
# otif_d_automation_output = Read_Kelvin_otif_d_Data(final_data_dict)


def get_into_primary_frame(driver):
    driver = sel.switch_frame_default(driver)
    xpath = "//*[@data-testid='chatty-parent']/iframe"
    [element, result] = sel.check_presence_of_element(driver, xpath, 100)
    if not result: raise
    [driver, result] = sel.switch_frame(driver, element)
    if not result: raise
    return driver


def click_otif_filter(driver):
    driver = get_into_primary_frame(driver)
    xpath = "//*[@class='toggleFilter']"
    [driver, result] = sel.click_element(driver, xpath, 20)
    if not result: raise
    return driver


class Read_otif_pharma_data:
    def __init__(self, final_data_dict, driver):
        # driver = stp.connect_webdriver()
        # driver = stp.kelvin_login(driver)

        # Navigate and authenticate OTIF pharma
        attempt = 0
        while attempt < 5:
            try:
                driver.get(conf.read_conf('kelvin', 'otif_d_pharma_url'))
                xpath = "//*[@type='submit']"
                [driver, result] = sel.click_element(driver, xpath, 4)
                if not result: raise
            except:
                driver = get_into_primary_frame(driver)
                xpath = "//*[text()='OTIF Pharma - Overview']"
                [element, result] = sel.check_presence_of_element(driver, xpath, 10)
                if not result: raise
                print(element.text)
                attempt = 7
            attempt = attempt + 1

        if attempt != 8:
            print("could not find OTIF Pharma source tool page loading")
            sys.exit(0)

        wait_time = 80
        driver = get_into_primary_frame(driver)

        xpath = "//*[@id='dashboardDynamic']/iframe"
        [element, result] = sel.check_presence_of_element(driver, xpath, 80)
        if not result: raise
        [driver, result] = sel.switch_frame(driver, element)
        if not result: raise

        xpath = "//*[text()='YTD']/../../.."
        [element, result] = sel.check_presence_of_element(driver, xpath, 80)
        if not result: raise
        # print(element.text)
        x = element.text.split("\n")
        ww_ytd = x[0]
        final_data_dict['WW'].append(ww_ytd)

        xpath = "//*[text()='Selected Months']/../../.."
        [element, result] = sel.check_presence_of_element(driver, xpath, 80)
        if not result: raise
        x = element.text.split("\n")
        ww_month = x[0]
        final_data_dict['WW'].append(ww_month)
        # print("WW : "+str(ww_ytd)+", "+str(ww_month))
        [driver, result] = sel.click_element(driver, xpath, 20)
        if not result: raise

        driver = sel.key_down(driver, 3, 1)

        # Extract region data for OTIF
        region = ['NA', 'EMEA', 'LATAM', 'APAC']
        for i in region:
            try:
                xpath = "(//*[text()='"+i+"'])[2]/../../.."
                [element, result] = sel.check_presence_of_element(driver, xpath, 80)
                if not result: raise
                x = element.text.split("\n")
                xpath = "(//*[text()='"+i+"'])[3]/../../.."
                [element, result] = sel.check_presence_of_element(driver, xpath, 80)
                if not result: raise
                y = element.text.split("\n")
                final_data_dict[i].append(y[0])
                final_data_dict[i].append(x[0])
                # print(i+" : "+str(x[0])+", "+str(y[0]))
            except Exception as e:
                print(e)
                
        otif_metric_l2 = ['Customer', 'Order Mgmt', 'Product Avail', 'Distribution', 'Transportation']

        # alias issued as the names from Kelvin is different from pharma page names
        otif_metric_l2_alias = {'Customer': 'Customer', 'Order Mgmt': 'Order Mgmt', 'Product Avail': 'Prod Avail', 'Distribution': 'Distribution', 'Transportation': 'Transportation'}
        for i in otif_metric_l2:

            xpath = "(//*[text()='"+i+"'])[3]/../../.."
            [element, result] = sel.check_presence_of_element(driver, xpath, 80)
            if not result: raise
            x = element.text.split("\n")
        #     print(x[0])
            xpath = "(//*[text()='"+i+"'])[4]/../../.."
            [element, result] = sel.check_presence_of_element(driver, xpath, 80)
            if not result: raise
            y = element.text.split("\n")
            final_data_dict['WW_'+str(otif_metric_l2_alias[i])].append(y[0])
            final_data_dict['WW_'+str(otif_metric_l2_alias[i])].append(x[0])
            # print(i+" : "+str(x[0])+", "+str(y[0]))
        print(final_data_dict)

        region = ["APAC", "NA", "EMEA", "LATAM"]
        for region_name in region:

            driver = get_into_primary_frame(driver)
            xpath = "//*[@class='toggleFilter']"
            [driver, result] = sel.click_element(driver, xpath, 10)
            if not result: raise

            xpath = "//*[text()='Reset']"
            [driver, result] = sel.click_element(driver, xpath, 40)
            if not result: raise

            driver = get_into_primary_frame(driver)
            xpath = "//*[@class='toggleFilter']"
            [driver, result] = sel.click_element(driver, xpath, 10)
            if not result: raise

            xpath = "//*[text()='FILTERS']"
            [driver, result] = sel.click_element(driver, xpath, 35)
            if not result: raise
            # print(element.text)

            attempt = 0
            while attempt < 9:
                try:
                    xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]"
                    [element, result] = sel.check_presence_of_element(driver, xpath, 3)
                    if not result: raise
                    print(element.text)
                    [driver, result] = sel.move_to_element(driver, element)
                    if not result: raise
                    attempt = 10
                except:
                    print(attempt)
                    attempt = attempt + 1

            print("Attempt", attempt)

            xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]"
            [element, result] = sel.check_presence_of_element(driver, xpath, 10)
            if not result: raise
            time.sleep(2)
            xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]/following-sibling::div"
            [element, result] = sel.check_presence_of_element(driver, xpath, 10)
            if not result: raise
            time.sleep(4)
            [driver, result] = sel.click_element(driver, xpath, 10)
            if not result: raise

            xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]/following-sibling::div//*[@class='dropdown-title']"
            [element, result] = sel.check_presence_of_element(driver, xpath, 30)
            if not result: raise
            [elements, result] = sel.find_elements(driver, xpath)
            if not result: raise
            # ytd_year_list = ['2024_01','2024_02']
            ytd_year_list = stp.get_year_till_ytd(conf.read_conf('kelvin', 'stockout_year_month'))

            pharma_ytd_year_list = []
            for i in elements:
                pharma_ytd_year_list.append(str(i.text))

            for i in pharma_ytd_year_list:
                if i in ytd_year_list:
                    # print("yes", i)
                    xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]/following-sibling::div//*[@class='dropdown-title'][text()='"+i+"']/preceding-sibling::input"
                    [selected_checkbox, result] = sel.check_presence_of_element(driver, xpath, 2)
                    if not result: raise
                    if selected_checkbox.is_selected():
                        pass
                    else:
                        selected_checkbox.click()
                        [driver, result] = sel.click_element(selected_checkbox, xpath, 10)
                        if not result: raise
                else:
                    # print("No", i)
                    xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]/following-sibling::div//*[@class='dropdown-title'][text()='"+i+"']/preceding-sibling::input"
                    [selected_checkbox, result] = sel.check_presence_of_element(driver, xpath, 2)
                    if not result: raise
                    if selected_checkbox.is_selected():
                        [driver, result] = sel.click_element(selected_checkbox, xpath, 10)
                        if not result: raise
                    else:
                        pass

            driver = get_into_primary_frame(driver)

            xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]"
            [element, result] = sel.check_presence_of_element(driver, xpath, 5)
            if not result: raise
            print(element.text)

            xpath = "(//*[@class='filter_container_body']//*[text()='Year Month - YTD'])[2]/following-sibling::div"
            [driver, result] = sel.click_element(driver, xpath, 5)
            if not result: raise

            xpath = "//*[@class='filter_container_body']//*[text()='Region']/following-sibling::div"
            [driver, result] = sel.click_element(driver, xpath, 20)
            if not result: raise
            xpath = "//*[@class='filter_container_body']//*[text()='"+region_name+"']"
            [driver, result] = sel.click_element(driver, xpath, 20)
            if not result: raise
            print(region_name)

            xpath = "//*[text()='Apply']"
            [driver, result] = sel.click_element(driver, xpath, 20)
            if not result: raise
            time.sleep(10)

            driver = get_into_primary_frame(driver)
            xpath = "//*[@id='dashboardDynamic']/iframe"
            [element, result] = sel.check_presence_of_element(driver, xpath, 5)
            if not result: raise
            [driver, result] = sel.switch_frame(driver, element)
            if not result: raise
            xpath = "//*[text()='YTD']/../../.."
            [element, result] = sel.check_presence_of_element(driver, xpath, 5)
            if not result: raise
            [driver, result] = sel.click_element(driver, xpath, 100)
            if not result:
                time.sleep(3)
                [driver, result] = sel.click_element(driver, xpath, 100)
                if not result: raise

            driver = sel.key_down(driver, 3, 4)

            otif_metric_l2 = ['Customer', 'Order Mgmt', 'Product Avail', 'Distribution', 'Transportation']
            otif_region = ['APAC', 'NA', 'EMEA', 'LATAM']
            wait_time = 80

            for i in otif_metric_l2:
                xpath = "(//*[text()='"+i+"'])[3]/../../.."
                [element, result] = sel.check_presence_of_element(driver, xpath, 80)
                if not result: raise
                x = element.text.split("\n")
                xpath = "(//*[text()='"+i+"'])[4]/../../.."
                [element, result] = sel.check_presence_of_element(driver, xpath, 80)
                if not result: raise
                y = element.text.split("\n")
                final_data_dict[str(region_name)+'_'+str(otif_metric_l2_alias[i])].append(y[0])
                final_data_dict[str(region_name)+'_'+str(otif_metric_l2_alias[i])].append(x[0])
        #         print(i+" : "+str(x[0])+", "+str(y[0]))
                self.final_data_dict = final_data_dict
        sel.kill_driver(driver)
# final_data_dict= {'WW': ['94.80%', '95.62%'], 'NA': ['95.50%', '94.90%'], 'EMEA': ['97.20%', '97.71%'], 'LATAM': ['N/A', 'N/A'], 'APAC': ['N/A', 'N/A'], 'WW_Customer': ['0.19%', '0.40%'], 'WW_Order Mgmt': ['0.70%', '1.19%'], 'WW_Prod Avail': ['1.64%', '1.91%'], 'WW_Distribution': ['N/A', 'N/A'], 'WW_Transportation': ['N/A', 'N/A'], 'NA_Customer': ['N/A', 'N/A'], 'NA_Order Mgmt': ['N/A', 'N/A'], 'NA_Prod Avail': ['N/A', 'N/A'], 'NA_Distribution': ['N/A', 'N/A'], 'NA_Transportation': ['N/A', 'N/A'], 'EMEA_Customer': ['N/A', 'N/A'], 'EMEA_Order Mgmt': ['N/A', 'N/A'], 'EMEA_Prod Avail': ['N/A', 'N/A'], 'EMEA_Distribution': ['N/A', 'N/A'], 'EMEA_Transportation': ['N/A', 'N/A'], 'LATAM_Customer': ['N/A', 'N/A'], 'LATAM_Order Mgmt': ['N/A', 'N/A'], 'LATAM_Prod Avail': ['N/A', 'N/A'], 'LATAM_Distribution': ['N/A', 'N/A'], 'LATAM_Transportation': ['N/A', 'N/A'], 'APAC_Customer': ['N/A', 'N/A'], 'APAC_Order Mgmt': ['N/A', 'N/A'], 'APAC_Prod Avail': ['N/A', 'N/A'], 'APAC_Distribution': ['N/A', 'N/A'], 'APAC_Transportation': ['N/A', 'N/A']}
# output_otif = get_otif_pharma_data(otif_d_automation_output.final_data_dict, otif_d_automation_output.driver)

# for i, j in output_otif.final_data_dict.items():
#     print(i, j)


def write_otif_to_data_frame(final_data_dict):
    # logger.info('Preparing stock out data frame')
    stock_outs_column_names = ["Date", "Regions", "YTD (Kelvin)", "Monthly (Kelvin)", "YTD (pharma)", "Monthly (pharma)"]
    lst = []
    for i, j in final_data_dict.items():
        k = []
        now = datetime.now()
        now = str(now).replace(":", "_")
        k.append(now)
        k.append(i)
        for f in final_data_dict[i]:
            k.append(f)
        lst.append(k)
    lst.append('')
    df = pd.DataFrame(lst, columns=stock_outs_column_names)
    df.columns = df.columns.str.upper()
    # print(df)
    stp.write_csv(df, conf.read_conf('kelvin', 'otif_d_output_file_path'))
    print("all done")

# final_data_dict = {'WW': ['92.66%', '92.66%', '92.49%', '92.49%'], 'NA': ['92.50%', '92.50%', '92.50%', '92.50%'], 'EMEA': ['91.24%', '91.24%', '91.24%', '91.24%'], 'LATAM': ['85.41%', '85.41%', '85.41%', '85.41%'], 'APAC': ['98.90%', '98.90%', '98.42%', '98.42%'], 'WW_Customer': ['0.44%', '0.44%', '0.44%', '0.44%'], 'WW_Order Mgmt': ['0.67%', '0.67%', '0.68%', '0.68%'], 'WW_Prod Avail': ['1.22%', '1.22%', '1.24%', '1.24%'], 'WW_Distribution': ['0.38%', '0.38%', '0.38%', '0.38%'], 'WW_Transportation': ['4.63%', '4.63%', '4.77%', '14.93%'], 'NA_Customer': ['0.45%', '0.45%', '0.45%', '0.45%'], 'NA_Order Mgmt': ['1.91%', '1.91%', '1.93%', '1.93%'], 'NA_Prod Avail': ['0.59%', '0.59%', '0.58%', '0.58%'], 'NA_Distribution': ['0.83%', '0.83%', '0.83%', '0.83%'], 'NA_Transportation': ['3.71%', '3.71%', '3.70%', '3.70%'], 'EMEA_Customer': ['0.41%', '0.41%', '0.41%', '0.41%'], 'EMEA_Order Mgmt': ['0.29%', '0.29%', '0.23%', '0.23%'], 'EMEA_Prod Avail': ['1.43%', '1.43%', '1.42%', '1.42%'], 'EMEA_Distribution': ['0.45%', '0.45%', '0.44%', '0.44%'], 'EMEA_Transportation': ['6.18%', '6.18%', '6.26%', '19.37%'], 'LATAM_Customer': ['1.89%', '1.89%', '1.89%', '1.89%'], 'LATAM_Order Mgmt': ['7.78%', '7.78%', '7.78%', '7.78%'], 'LATAM_Prod Avail': ['3.60%', '3.60%', '3.60%', '3.60%'], 'LATAM_Distribution': ['0.26%', '0.26%', '0.26%', '0.26%'], 'LATAM_Transportation': ['1.05%', '1.05%', '1.05%', '1.05%'], 'APAC_Customer': ['0.26%', '0.26%', '0.26%', '0.26%'], 'APAC_Order Mgmt': ['0.41%', '0.41%', '0.66%', '0.66%'], 'APAC_Prod Avail': ['0.25%', '0.25%', '0.33%', '0.33%'], 'APAC_Distribution': ['0.01%', '0.01%', '0.02%', '0.02%'], 'APAC_Transportation': ['0.17%', '0.17%', '0.31%', '0.31%']}
# write_otif_to_data_frame(final_data_dict)


def otif_d_qa_automation():
    otif_d_kelvin_data = Read_Kelvin_otif_d_Data({})
    print(otif_d_kelvin_data.final_data_dict)
    otif_d_pharma_data = Read_otif_pharma_data(otif_d_kelvin_data.final_data_dict, otif_d_kelvin_data.driver)
    print(otif_d_pharma_data.final_data_dict)
    write_otif_to_data_frame(otif_d_pharma_data.final_data_dict)


# if __name__ == '__main__':
#     # Install python packages or dependencies from the file called 'install+packages.py'
#     ip.InstallPackages()
#     # logger.info("Installed all the dependencies and packages")
#     from datetime import datetime
#     # now = datetime.now()
#     # print(now)
#     stp.set_month_and_year_to_confluence()

otif_d_qa_automation()

