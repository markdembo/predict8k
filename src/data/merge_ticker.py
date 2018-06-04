"""Merge filings data with Compustat data."""
import pandas as pd
from fuzzywuzzy import fuzz


def merge_computstat_filings(df, fname):
    """Merge filings information with Compustat data."""
    ticker_df = pd.read_csv(fname)

    merged_df = pd.merge(
        df,
        ticker_df[["cik", "ticker", "name"]],
        how="inner",
        on="cik",
    )

    merged_df["match"] = (
        merged_df.apply(lambda x: fuzz.ratio(
                x["name_x"].replace('[^A-Za-z\s]+', '').lower(),
                x["name_y"].replace('[^A-Za-z\s]+', '').lower()
            ),
            axis=1)
    )

    return merged_df.loc[merged_df.match > 60]


def main(df, ticker_path, logger):
    """Merge filings with Compustat data."""
    output = merge_computstat_filings(df,
                                      ticker_path,
                                      )
    return output
