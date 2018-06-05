"""Merge filings data with Compustat data."""
import pandas as pd
from fuzzywuzzy import fuzz


def merge(filings_df, citicker_df):
    """Merge filings information with Compustat data."""
    merged_df = pd.merge(
        filings_df,
        citicker_df[["cik", "ticker", "name"]],
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


def main(filings_df, citicker_df, logger):
    """Merge filings with Compustat data."""
    output = merge(filings_df, citicker_df)
    return output
