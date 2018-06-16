"""Prepare WRDS query."""
import io
import re
import pandas as pd


def output_for_wrds(df, n):
    """Save txt files as input for wrds queries."""
    output_list = []
    list_df = [df[i:i+n] for i in range(0, df.shape[0], n)]
    for chunk in list_df:
        extract = chunk[["ticker", "date_accepted"]].copy()
        extract["datetime"] = pd.to_datetime(extract.date_accepted,
                                             format="%Y%m%d%H%M%S")
        del extract["date_accepted"]

        output = io.StringIO()
        extract.drop_duplicates(inplace=True)
        extract.to_csv(output, date_format="%Y%m%d %H:%M:%S",
                       index=False, header=False, sep=" ")
        output_edits = re.sub("\"", "", output.getvalue())
        output_edits = re.sub("\.\S?", "", output_edits)
        output_list.append(output_edits)

    return output_list


def main(df, n, logger):
    """Prepare WRDS query."""
    query_inputs = output_for_wrds(df, n)
    return query_inputs
