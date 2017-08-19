"""Merge filings data with Compustat data."""
import os
import logging
import logging.config
import glob
import io
import re
import pandas as pd
import numpy as np
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm, trange

SAMPLE_START_YEAR = 2015
SAMPLE_END_YEAR = 2016
COMPUSTAT_PATH = "data/external/"
COMPUSTAT_PATTERN = "Compustat_YYYY.csv"
EXTRACTS_PATH = "data/interim/"
EXTRACTS_PATTERN = "*_extract.csv"

WRDS_PATH = "data/interim/"
WRDS_SUFFIX = "_wrdsinput"
MERGE_PATH = "data/interim/"
MERGE_SUFFIX = "_merge"


def merge_computstat_filings(sample_start_year, sample_end_year,
                             compustat_path, compustat_pattern,
                             extracts_path, extracts_pattern):
    """Merge filings information with Compustat data."""
    compustat = {}
    for year in trange(sample_start_year, sample_end_year + 1):
        search = compustat_path + compustat_pattern.replace("YYYY", str(year))
        try:
            filepath = glob.glob(search)[0]
        except IndexError:
            print("No file for the year %s with the name %s found."
                  % (year, compustat_pattern.replace("YYYY", str(year))))
        compustat[year] = pd.read_csv(filepath)

    files = glob.glob(extracts_path + extracts_pattern)
    merge_dfs = {}
    for f in tqdm(files):
        df = pd.read_csv(f, index_col=0, dtype=str)
        df.date = pd.to_datetime(df.date_filed)
        temp_dfs = []
        for year in df.date.dt.year.unique():
            if np.isnan(year):
                continue
            df = df.loc[df.date.dt.year == year].copy()
            df.cik = df.cik.fillna(0).astype(int)
            temp_dfs.append(pd.merge(compustat[int(year)], df, on="cik"))
        merge_dfs[f] = pd.concat(temp_dfs)
    return merge_dfs


def output_merge(merge_dfs, extracts_pattern, merge_path, merge_suffix):
    """Save the merged dataframes."""
    for key, item in tqdm(list(merge_dfs.items())):
        filename_full = os.path.basename(key)
        old_filename = os.path.splitext(filename_full)[0]
        old_suffix = os.path.splitext(extracts_pattern.replace("*", ""))[0]
        new_filefname = old_filename.replace(old_suffix, merge_suffix) + ".csv"
        output_filename_full = merge_path + new_filefname
        item.to_csv(output_filename_full, encoding="utf-8")


def output_for_wrds(merge_dfs, extracts_pattern, wrds_path, wrds_suffix):
    """Save txt files as input for wrds queries."""
    for key, df in tqdm(list(merge_dfs.items())):
        extract = df[["tic", "date_accepted"]].copy()
        extract["datetime"] = pd.to_datetime(extract.date_accepted,
                                             format="%Y%m%d%H%M%S")
        del extract["date_accepted"]
        filename_full = os.path.basename(key)
        old_filename = os.path.splitext(filename_full)[0]
        old_suffix = os.path.splitext(extracts_pattern.replace("*", ""))[0]
        new_filefname = old_filename.replace(old_suffix, wrds_suffix) + ".txt"
        output_filename_full = wrds_path + new_filefname
        output = io.StringIO()
        extract.drop_duplicates(inplace=True)
        extract.to_csv(output, date_format="%Y%m%d %H:%M:%S",
                       index=False, header=False, sep=" ")
        output_edits = re.sub("\"", "", output.getvalue())
        output_edits = re.sub("\.\S?", "", output_edits)
        with open(output_filename_full, "w") as f:
            f.write(output_edits)


def main(sample_start_year, sample_end_year, compustat_path, compustat_pattern,
         extracts_path, extracts_pattern, merge_path, merge_suffix, wrds_path,
         wrds_suffix):
    """Consolidate filings into csv files."""
    logger.info('Downloading data from EDGAR database')
    dfs = merge_computstat_filings(sample_start_year, sample_end_year,
                                   compustat_path, compustat_pattern,
                                   extracts_path, extracts_pattern)
    output_merge(dfs, extracts_pattern, merge_path, merge_suffix)
    output_for_wrds(dfs, extracts_pattern, wrds_path, wrds_suffix)
    output_for_wrds(dfs, extracts_pattern, wrds_path, wrds_suffix)


if __name__ == "__main__":
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger(__name__)
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main(SAMPLE_START_YEAR, SAMPLE_END_YEAR, COMPUSTAT_PATH, COMPUSTAT_PATTERN,
         EXTRACTS_PATH, EXTRACTS_PATTERN, MERGE_PATH, MERGE_SUFFIX,
         WRDS_PATH, WRDS_SUFFIX)
