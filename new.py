import pandas as pd

f = "E:\thesis\predict8k\data\interim\filings_2018-02_8-K_queryoutput.csv"
df = pd.read_csv(f)
query_results = df.output

dfs = [pd.read_csv(file) for file in query_results]
consildated_df = pd.concat(dfs)
