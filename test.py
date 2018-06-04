import pandas as pd
import re
from fuzzywuzzy import fuzz

filings = pd.read_csv("data/interim/filings_2018-02_8-K_extract.csv", index_col=0)

# Correct corrupted file
pattern = "(,(\d*),(.+),(.*),(.*,.*,))"
with open("data/external/cik_ticker.csv", "r") as f:
    content = f.read()

for match in re.findall(pattern, content):
    correction = ",{},{}{},{}".format(match[1], match[2], match[3], match[4])
    content = content.replace(match[0], correction)

with open("data/external/cik_ticker_edit.csv", "w") as f:
    f.write(content)

cik_ticker = pd.read_csv("data/external/cik_ticker_edit.csv")

filings.columns
cik_ticker.columns

merged_df = pd.merge(filings, cik_ticker[["cik", "ticker", "name"]], how="inner", on="cik")

merged_df[["name_x", "name_y"]]

merged_df["match"] = (merged_df.apply(lambda x:
    fuzz.ratio(
        x["name_x"].replace('[^A-Za-z\s]+', '').lower(),
        x["name_y"].replace('[^A-Za-z\s]+', '').lower()
    ), axis=1)

merged_df.loc[merged_df.match > 60]
