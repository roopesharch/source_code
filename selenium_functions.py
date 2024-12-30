from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

import sys
import time

# logger = stp.log('stockout_path')


def kill_driver(driver):
    try:
        driver.quit()
        return
    except Exception as e:
        # logger.error(e)
        print(e)
        sys.exit("Driver quite function not working")


def get_url(driver, url):
    try:
        driver.get(url)
        return driver
    except Exception as e:
        # logger.error(e)
        print(e)
        sys.exit("Driver get function not working")


def click_element(driver, xpath, wait_time):
    try:
        element = WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.XPATH, xpath)))
        element.click()
        return [driver, True]

    except Exception as e:
        print(e)
        return [driver, False]


def send_input(driver, xpath, wait_time, value):
    try:
        element = WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.XPATH, xpath)))
        element.send_keys(value)
        return [driver, True]

    except Exception as e:
        print(e)
        return [driver, False]


def check_presence_of_element(driver, xpath, wait_time):
    try:
        element = WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.XPATH, xpath)))
        return [element, True]

    except Exception as e:
        print(e)
        return [driver, False]


def find_elements(driver, xpath):
    try:
        elements = driver.find_elements(By.XPATH, xpath)
        return [elements, True]
    except Exception as e:
        print(e)
        return [elements, False]


def switch_frame(driver, element):
    try:
        driver.switch_to.frame(element)
        return [driver, True]
    except Exception as e:
        print(e)
        return [driver, False]


def switch_frame_default(driver):
    try:
        driver.switch_to.default_content()
        return driver
    except Exception as e:
        # logger.error(e)
        print(e)
        sys.exit("Driver switch function not working")


def key_down(driver, no_of_press_down, wait_time):
        action = ActionChains(driver)
        for i in range(no_of_press_down):
            action.key_down(Keys.PAGE_DOWN).perform()
            time.sleep(wait_time)
        return driver

def key_up(driver, no_of_press_down, wait_time):
        action = ActionChains(driver)
        for i in range(no_of_press_down):
            action.key_down(Keys.PAGE_UP).perform()
            time.sleep(wait_time)
        return driver


def move_to_element(driver, element):
    try:
        action = ActionChains(driver)
        action.move_to_element(element).perform()
        action.key_down(Keys.PAGE_DOWN).perform()
        return [driver, True]
    except Exception as e:
        print(e)
        return [driver, False]


def send_esc(driver):
    try:
        action = ActionChains(driver)
        action.key_down(Keys.ESCAPE).perform()
        return [driver, True]
    except Exception as e:
        print(e)
        return [driver, False]


def current_window_handle_return(driver):
    try:
        return [driver.current_window_handle, True]
    except Exception as e:
        print(e)
        return [driver, False]


def window_handles_return(driver):
    try :
        return [driver.window_handles, True]
    except Exception as e:
        print(e)
        return [driver, False]


def switch_to_window(driver, window):
    try:
        driver.switch_to.window(window)
        return [True]
    except Exception as e:
        print(e)
        return [False]
