"""Prepare WRDS query."""
import io
import re
import pandas as pd


def output_for_wrds(df):
    """Save txt files as input for wrds queries."""
    extract = df[["tic", "date_accepted"]].copy()
    extract["datetime"] = pd.to_datetime(extract.date_accepted,
                                         format="%Y%m%d%H%M%S")
    del extract["date_accepted"]

    output = io.StringIO()
    extract.drop_duplicates(inplace=True)
    extract.to_csv(output, date_format="%Y%m%d %H:%M:%S",
                   index=False, header=False, sep=" ")
    output_edits = re.sub("\"", "", output.getvalue())
    output_edits = re.sub("\.\S?", "", output_edits)
    return output_edits


def main(df, logger):
    """Prepare WRDS query."""
    query_input = output_for_wrds(df)
    return query_input
