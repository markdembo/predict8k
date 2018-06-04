"""Consolidate 8-k filings into a csv file."""
import pandas as pd


def readandstore(row):
    """Open file and return content."""
    with open(row["local_fname"]) as f:
        txt = f.read()
    return txt


def main(input, logger):
    """Consolidate filings into csv files."""
    logger.info('Extracting content from filings')
    try:
        filterdf = input.loc[input.seq < 2].copy()
        filterdf["content"] = filterdf.apply(readandstore, axis=1)
    except Exception as e:
        logger.error(e)

    return filterdf
