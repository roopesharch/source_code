
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Use the below line to create a build
# pyinstaller  main.py --onefile

from source import install_packages as ip
from source import setep as stp
from source import read_config as conf
from source import selenium_functions as sel
import time


class kelvin_home_sanity_check:
    def __init__(self, home_sanity_output):

        driver = stp.connect_webdriver()
        driver = stp.kelvin_login(driver,  conf.read_conf('kelvin', 'kelvin_home_page_url'))

        # check if score card is active
        def check_active_scorecards():
            score_card_list = ['Deliver', 'PCMD', 'JSC', 'Brand Performance']

            for i in score_card_list:
                try:
                    xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])[1]"
                    # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,xpath)))
                    [element, result] = sel.check_presence_of_element(driver, xpath, 40)
                    if not result: raise
                    # print(element.text)
                    print("Normal user visible for " + i)
                except Exception as e:
                    print("Normal user not visible for " + i)
                    print(e)
                try:
                    xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])[2]"
                    # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,xpath)))
                    [element, result] = sel.check_presence_of_element(driver, xpath, 40)
                    if not result: raise
            #         print(element.text)
                    print("Super user visible for " + i)
                except Exception as e:
                    print("Super user not visible for " + i)
                    print(e)

            # check if score card is available for all the months
            score_card_list = ['Deliver', 'PCMD', 'JSC', 'Brand Performance']
            updated_monthly = 'UPDATED MONTHLY'

            for i in score_card_list:
                try:
                    xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])[1]/following-sibling::div/p"
                    # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,xpath)))
                    [element, result] = sel.check_presence_of_element(driver, xpath, 40)
                    if not result: raise

                    if updated_monthly in str(element.text):
                        print(element.text+ " data for " + i +" Normal User")
                    else:
                        print('no for', i)
                except Exception as e:
                    print("Monthly available not visible")
                    print(e)

                try:
                    xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])[2]/following-sibling::div/p"
                    # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,xpath)))
                    [element, result] = sel.check_presence_of_element(driver, xpath, 40)
                    if not result: raise
                    if updated_monthly in str(element.text):
                        print(element.text + " for " + i+" Super User")
                    else:
                        print('no for', i)

                except Exception as e:
                    print("Monthly available not visible")
                    print(e)

        check_active_scorecards()

        def check_date():
            date = conf.read_conf('kelvin', 'kelvin_date')
            # print(date)

            xpath="//div[@class='months']/a[@class='active'][contains(text(),'"+stp.get_month_name_in_words(date[4:])+"')]"
            try:
                # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, xpath)))
                [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                if not result: raise
                print("Right date selected")
            except Exception as e:
                print('Not now fail', e)

            xpath = "//select/option[contains(text(),'"+date[:4]+"')]"
            try:
                # element = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,xpath)))
                [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                if not result: raise
                print("Right year selected")
            except Exception as e:
                print('Not now fail', e)

        check_date()

        def check_score_cards_navigation_from_home(user_type):
            if user_type == 1:
                usr = ' Normal User '
            elif user_type == 2:
                usr = ' Super User'
            score_card_list = ['Deliver', 'PCMD', 'JSC', 'Brand Performance']

            for i in score_card_list:

                if 'Brand' in i:
                    try:
                        xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])["+str(user_type)+"]"
                        # element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                        # element.click()
                        [element, result] = sel.click_element(driver, xpath, 20)
                        if not result: raise
                        print(usr+" clicked for " + i)
                    except Exception as e:
                        print(usr+" not visible for " + i)
                        print(e)

                    try:
                        xpath = "//div[text()='Please select a brand to see data']"
                        # element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                        [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                        if not result: raise
                        print(usr+" loaded for " + i)
                    except Exception as e:
                        print(usr+" not loaded for " + i)
                        print(e)

                    xpath = "(//button)[2]"
                    try:
                        xpath = "(//button)[2]"
                        # element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                        # element.click()
                        [element, result] = sel.click_element(driver, xpath, 20)
                        if not result: raise
                        print("clicked back button for " + i)
                    except Exception as e:
                        print(usr+" not back button for " + i)
                        print(e)
                else:

                    try:
                        xpath = "(//*[@class='MuiGrid-root scorecardContainer FinalContainer css-rfnosa']/*[text()='"+i+"'])["+str(user_type)+"]"
                        # element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                        # element.click()
                        [element, result] = sel.click_element(driver, xpath, 20)
                        if not result: raise
                        print(usr+" clicked for " + i)
                    except Exception as e:
                        print(usr+" not visible for " + i)
                        print(e)

                    try:
                        xpath = "//p[contains(text(),'"+i+" Scorecard')]"
                        # element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                        [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                        if not result: raise
                        print(usr+" loaded for " + i)
                    except Exception as e:
                        print(usr+" not loaded for " + i)
                        print(e)

                    check_date()

                    xpath = "//p[contains(text(),'PCMD Scorecard')]/preceding-sibling::button"
                    try:
                        xpath = "(//button)[2]"
                #         xpath = "//p[contains(text(),'"+i+" Scorecard')]/preceding-sibling::button"
                #         element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath)))
                #         element.click()
                        [element, result] = sel.click_element(driver, xpath, 20)
                        if not result: raise
                        print("clicked back button for " + i)
                    except Exception as e:
                        print(usr+" not back button for " + i)
                        print(e)

                xpath="//div[@class='MuiGrid-root css-rfnosa']/button"
                # element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, xpath)))
                [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                if not result: raise
                print(element.text)
                check_date()

        check_score_cards_navigation_from_home(1)
        check_score_cards_navigation_from_home(2)

        def bun_menu_navigation_checks():
            bun_menu_content={'Update Supplier OTIF data': 'jds.jnj', 'Update Brand Mapping data': 'jds.jnj', 'Update MAPE data': 'jds.jnj', 'Update MIN/MAX data': 'jds.jnj', 'Update Capex data': 'jds.jnj', 'Kelvin Metric Owner': 'sharepoint'}

            for i, j in bun_menu_content.items():
            #     print(i,j)

                current_tab = driver.current_window_handle
                [current_tab, result] = sel.current_window_handle_return(driver)
                if not result: raise

                xpath = "//div[@class='MuiGrid-root css-rfnosa']/button"
                try:
                    # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,xpath)))
                    # element.click()
                    [element, result] = sel.click_element(driver, xpath, 20)
                    if not result: raise
                    print("yes")
                except Exception as e:
                    print('No', e)
                time.sleep(2)
            #     xpath = "//ul[@role='menu']/li"
                xpath = "//ul[@role='menu']/li[contains(text(),'"+i+"')]"
                try:
                    # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
                    # print(i)
                    # element.click()
                    [element, result] = sel.click_element(driver, xpath, 20)
                    if not result: raise

                    #get first child window
                    # tabs_list = driver.window_handles
                    [tabs_list, result] = sel.window_handles_return(driver)
                    if not result: raise
                    for window in tabs_list:
                        #switch focus to child window
                        if window != current_tab:
                            # driver.switch_to.window(window)
                            [result] = sel.switch_to_window(driver, window)
                            if not result: raise
                    time.sleep(3)
                    if j in str(driver.current_url):
                        print(i+" has correct link navigation")
                    print(driver.current_url)
                    driver.close()
                    # driver.switch_to.window(current_tab)
                    [result] = sel.switch_to_window(driver, current_tab)
                    if not result: raise
                    time.sleep(3)

                    xpath="//div[@class='MuiGrid-root css-rfnosa']/button"
                    # element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, xpath)))
                    [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                    if not result: raise
                    print(element.text)
                    check_date()
                except Exception as e:
                    print('No', )
        bun_menu_navigation_checks()

        def check_analytics_navigation_from_home():
            # usage analytics check

            xpath ="//button/p[contains(text(),'Analytics')]"
            try:
                # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,xpath)))
                # element.click()
                [element, result] = sel.click_element(driver, xpath, 20)
                if not result: raise
                print("clicked analytics")
            except Exception as e:
                print('No', e)

            xpath = "//div[contains(text(),'Usage Analytics')]"
            try :
                # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,xpath)))
                [element, result] = sel.check_presence_of_element(driver, xpath, 25)
                if not result: raise
                print(element.text)
            except Exception as e:
                print('No', e)

            xpath = "(//button)[2]"
            try :
                # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,xpath)))
                # element.click()
                [element, result] = sel.click_element(driver, xpath, 20)
                if not result: raise
                print("Back")
            except Exception as e:
                print('No', e)

            xpath="//div[@class='MuiGrid-root css-rfnosa']/button"
            # element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, xpath)))
            [element, result] = sel.check_presence_of_element(driver, xpath, 25)
            if not result: raise
            print(element.text)
            check_date()
        check_analytics_navigation_from_home()
        driver.quit()


if __name__ == '__main__':
    # Install python packages or dependencies from the file called 'install+packages.py'
    ip.InstallPackages()
    # logger.info("Installed all the dependencies and packages")
    from datetime import datetime
    now = datetime.now()
    print(now)
    kelvin_home_sanity_check({})















