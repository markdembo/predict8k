"""Prepare WRDS query."""
import io
import re
import pandas as pd
import os
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def make_request(url, user, password, f, download_path, timeout, logger):
    """Automatically fill out the query form online."""
    corrected_path = os.path.abspath(download_path) + r"\\"
    custom_options = webdriver.ChromeOptions()
    custom_options.add_experimental_option(
        "prefs",
        {
            "download.directory_upgrade": True,
            "download.default_directory": corrected_path,
            "download.prompt_for_download": False,
            #    "safebrowsing.enabled": True,
        }
    )
    driver = webdriver.Chrome(options=custom_options)
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

    # Enter value in first section
    elem_start = driver.find_element_by_name("begtime")
    elem_start.clear()
    elem_start.send_keys("-600")

    # Deselect variables
    elem_cum_median = (
        driver
        .find_element_by_xpath("//*[@id='t_varByGroupId39182_158674']"))
    driver.execute_script("arguments[0].style.visibility = 'visible';",
                          elem_cum_median)
    driver.execute_script("arguments[0].style.margin = '0';", elem_cum_median)
    elem_cum_median.click()

    elem_cum_mean = (
        driver
        .find_element_by_xpath("//*[@id='t_varByGroupId39182_158675']"))
    driver.execute_script("arguments[0].style.visibility = 'visible';",
                          elem_cum_mean)
    driver.execute_script("arguments[0].style.margin = '0';", elem_cum_mean)
    elem_cum_mean.click()

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
    wait.until((
        EC.text_to_be_present_in_element((By.XPATH,
                                          "//*[@id='main']/p[1]"),
                                         "Your output is complete")))
    a_csv = elemsubmit = driver.find_element_by_xpath('//*[@id="main"]/p[2]/a')
    link_text = a_csv.text
    a_csv.click()
    local_path = os.path.relpath(download_path + link_text)
    try:
        x = 0
        while not os.path.exists(local_path):
            time.sleep(1)
            x += 1
            if x >= timeout:
                break
    except Exception:
        logger.error("TAQ download timeout")
    driver.close()
    driver.quit()
    return local_path


def prep_query(df, n):
    """Save txt files as input for wrds queries."""
    output_list = []
    list_df = [df[i:(i + n)] for i in range(0, df.shape[0], n)]
    for chunk in list_df:
        extract = chunk[["ticker", "date_accepted"]].copy()
        extract["datetime"] = pd.to_datetime(
            extract.date_accepted, format="%Y%m%d%H%M%S"
        )
        del extract["date_accepted"]

        output = io.StringIO()
        extract.drop_duplicates(inplace=True)
        extract.to_csv(
            output,
            date_format="%Y%m%d %H:%M:%S",
            index=False,
            header=False,
            sep=" ",
        )
        output_edits = re.sub('"', "", output.getvalue())
        output_edits = re.sub("\.\S?", "", output_edits)
        output_list.append(output_edits)
    return output_list


def run_query(filelist, url, user, password, download_path, timeout, logger):
    """Download return data."""
    result = pd.DataFrame(columns=("input", "output"))
    for f in filelist:
        filepath = make_request(url,
                                user,
                                password,
                                os.path.abspath(f),
                                download_path,
                                timeout,
                                logger,
                                )
        result = result.append([{
            "input": f,
            "output": filepath,
        }])
    return result


def consolidate_results(df):
    query_results = df.output
    dfs = [pd.read_csv(file) for file in query_results]
    consildated_df = pd.concat(dfs)
    return consildated_df
