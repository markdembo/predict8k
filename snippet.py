import pandas as pd

df = pd.read_csv("data/interim/filings_2018-02_8-K_queryoutput.csv")

df.time
for f in df.time:
    print(f)
