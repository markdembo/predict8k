"""Consolidate 8-k filings into a csv file."""
import os
import logging
import logging.config
import pandas as pd
import glob
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm

FILINGS_PATH = "data/raw/edgar/"
FILINGS_PATTERN = "filings*.csv"
OUTPUT_PATH = "data/interim/"
OUTPUT_SUFFIX = "_content"


def readandstore(row):
    """Open file and return content."""
    with open(row["local_fname"]) as f:
        txt = f.read()
    return txt


def main(filingspath, filingspattern, outputpath, suffix):
    """Consolidate filings into csv files."""
    logger.info('Downloading data from EDGAR database')
    all_docs = glob.glob(filingspath + filingspattern)
    for doc in tqdm(all_docs):
        df = pd.read_csv(doc, index_col=0)
        filterdf = df.loc[df.seq < 2].copy()
        filterdf["content"] = filterdf.apply(readandstore, axis=1)

        filename_full = os.path.basename(doc)
        filename = os.path.splitext(filename_full)[0]
        output_filename = outputpath + filename + suffix + ".csv"
        filterdf.to_csv(output_filename, encoding="utf-8")


if __name__ == "__main__":
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger(__name__)
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main(FILINGS_PATH, FILINGS_PATTERN, OUTPUT_PATH, OUTPUT_SUFFIX)
