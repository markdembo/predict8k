import re
import pandas as pd

df = pd.read_csv("data/interim/filings_2018-02_content.csv", encoding="utf-8").reset_index()

header_df = df.loc[df.seq == 0].copy()
splitdf = header_df.content.str.split("FILER:", expand=True)
splitdf
search_sec = (
    "^.*?<ACCEPTANCE-DATETIME>(?P<DATE_ACCEPTED>.+?)\\r\\n"
    "ACCESSION NUMBER:[\\t]+(?P<ACCESSION_NO>.+?)\\r\\n"
    "CONFORMED SUBMISSION TYPE:[\\t]+(?P<SUB_TYPE>.+?)\\r\\n"
    "PUBLIC DOCUMENT COUNT:[\\t]+(?P<DOC_COUNT>.+?)\\r\\n"
    "CONFORMED PERIOD OF REPORT:[\\t]+(?P<REPORT_PERIOD>\d+?)\\r\\n"
    ".*?ITEM INFORMATION:[\\t]+(?P<ITEM_INFO>.+?)[\\r\\n|.]+?"
    "FILED AS OF DATE:[\\t]+(?P<DATE_FILED>.+?)\\r\\n"
    "(DATE AS OF CHANGE:[\\t]+(?P<CHANGE_DATE>\d+)?[\\r\\n]*)?")

result = pd.read_csv("df.csv", header=None)[1].str.extract(search, expand=False, flags=re.DOTALL)
result

with open("search.txt") as f:
    search = f.read()

search
