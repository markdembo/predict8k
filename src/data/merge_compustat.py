"""Merge filings data with Compustat data."""
import pandas as pd


def merge_computstat_filings(df, compustat_path, compustat_pattern):
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
    """Merge filings with Compustat data."""
    output = merge_computstat_filings(df,
                                      compustat_path,
                                      compustat_pattern)
    return output
