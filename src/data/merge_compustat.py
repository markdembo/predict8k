"""Merge filings data with Compustat data.

TODO: Update documentation

"""
import pandas as pd

def merge_computstat_filings(df,
                             compustat_path,
                             compustat_pattern,
                             ):
    """Merge filings information with Compustat data."""
    year = (
        pd.to_datetime(df.date_filed.astype(str),
                       format="%Y%m%d")
        .dt
        .year
        .unique()[0]
    )
    fname = (
        compustat_path
        + compustat_pattern.replace("YYYY", str(year))
    )

    compustat_df = pd.read_csv(fname)

    df.cik = df.cik.fillna(0).astype(int)
    return pd.merge(compustat_df, df, on="cik")


def main(df, compustat_path, compustat_pattern, logger):
    """Consolidate filings into csv files."""
    logger.info('Downloading data from EDGAR database')
    output = merge_computstat_filings(df,
                                      compustat_path,
                                      compustat_pattern)
    return output
