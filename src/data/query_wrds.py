"""Request taq data.

TODO: Understand files
TODO: Make pipeline compatible
TODO: Update documentation
TODO: Strip module imports
TODO: Add download folder management

"""
import os
# import logging
# import logging.config
# import glob
import time
import pandas as pd
import re
# from dotenv import find_dotenv, load_dotenv
# from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

"""
# FIXME: Update to fit luigi approach
def get_files(wrds_path, wrds_suffix):
    ""Find all input files for the query.""
    input_files = glob.glob(wrds_path + "*" + wrds_suffix + ".txt")
    abs_files = [os.path.abspath(f) for f in input_files]
    return abs_files
"""


def make_request(url, user, password, f, download_path):
    """Automatically fill out the query form online."""
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True})
    driver = webdriver.Chrome()
    driver.get(url)
    main_window_handle = None
    while not main_window_handle:
        main_window_handle = driver.current_window_handle

    # Login
    elemuser = driver.find_element_by_name("username")
    elemuser.clear()
    elemuser.send_keys(user)
    elempw = driver.find_element_by_name("password")
    elempw.clear()
    elempw.send_keys(password)
    elemsubmit = driver.find_element_by_xpath('//button[text()="Submit"]')
    elemsubmit.send_keys(Keys.RETURN)

    # Make query
    elem_file = driver.find_element_by_name("file_to_upload")
    elem_file.clear()
    elem_file.send_keys(f)

    # Expand first section
    cola1 = (
        driver
        .find_element_by_xpath("//*[@id='wrdsqueryform']/div/div[2]/div[1]/a"))
    cola1.click()
    time.sleep(0.1)

    # Enter value in first section
    elem_start = driver.find_element_by_name("begtime")
    elem_start.clear()
    elem_start.send_keys("-600")

    # Expand second section
    cola2 = (
        driver
        .find_element_by_xpath("//*[@id='wrdsqueryform']/div/div[3]/div[1]/a"))
    cola2.click()
    time.sleep(0.1)

    # Deselect variables
    elem_cum_median = (
        driver
        .find_element_by_xpath("//*[@id='t_varByGroupId39182_CUM_MEDIAN']"))
    driver.execute_script("arguments[0].style.visibility = 'visible';",
                          elem_cum_median)
    driver.execute_script("arguments[0].style.margin = '0';", elem_cum_median)
    elem_cum_median.click()

    elem_cum_mean = (
        driver
        .find_element_by_xpath("//*[@id='t_varByGroupId39182_CUM_MEAN']"))
    driver.execute_script("arguments[0].style.visibility = 'visible';",
                          elem_cum_mean)
    driver.execute_script("arguments[0].style.margin = '0';", elem_cum_mean)
    elem_cum_mean.click()

    # Expand third section
    cola3 = (
        driver
        .find_element_by_xpath("//*[@id='wrdsqueryform']/div/div[4]/div[1]/a"))
    cola3.click()
    time.sleep(0.1)

    # Select output type and enter custom query name
    elem_csv = (
        driver
        .find_element_by_xpath("//*[@id='csv']"))
    elem_csv.click()

    elempw = driver.find_element_by_name("custom_field")
    elempw.clear()
    elempw.send_keys("predict8k")

    # Submit query
    elemfsubmit = driver.find_element_by_id("form_submit")
    elemfsubmit.send_keys(Keys.RETURN)
    signin_window_handle = None
    while not signin_window_handle:
        for handle in driver.window_handles:
            if handle != main_window_handle:
                signin_window_handle = handle
                break
    # Wait and download
    driver.switch_to.window(signin_window_handle)
    wait = WebDriverWait(driver, 400, poll_frequency=1)
    query_done = wait.until((
        EC.text_to_be_present_in_element((By.XPATH,
                                         "//*[@id='main']/p[1]"),
                                         "Your output is complete")))
    a_csv = elemsubmit = driver.find_element_by_xpath('//*[@id="main"]/p[2]/a')
    link_text = a_csv.text
    a_csv.click()
    time.sleep(60)
    driver.close()
    driver.quit()
    return link_text


def main(f, url, user, password, download_path, logger):
    """Download return data."""

    """
    files = get_files(wrds_path, wrds_suffix)

    # corrected_path = re.sub(r"\\", "/",
    """

    """
    for f in tqdm(files):
        tqdm.write("Making request using %s as input file." % f)
        filename = make_request(url, user, password, f, corrected_path)
        res = res.append([{"file": f, "time": filename}])
    """
    res = pd.DataFrame(columns=("file", "time"))
    corrected_path = os.path.abspath(download_path) + r"\\"
    filename = make_request(url, user, password, os.path.abspath(f), corrected_path)
    res = res.append([{"file": f, "time": filename}])
    return res
