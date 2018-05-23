import re
import pandas as pd

with open("data/interim/filings_2018-02_content.csv", mode="r") as f:
    df = pd.read_csv(f, encoding="utf-8").reset_index()


repr(df.content[0])

def series_extract(series, search, flag):
    """Extract named groups from strings."""
    result = series.str.extract(search, expand=False, flags=re.DOTALL)
    no_integers = [x for x in result.columns.values if not isinstance(x, int)]
    return result[no_integers].copy()


def get_headerfiles(df):
    """Extract header files only from documents."""
    header_df = df.loc[df.seq == 0].copy()
    splitdf = header_df.content.str.split("FILER:", expand=True)
    return splitdf


search_sec = (
    "^.*?<ACCEPTANCE-DATETIME>(?P<DATE_ACCEPTED>.+?)\\r\\n"
    "ACCESSION NUMBER:[\\t]+(?P<ACCESSION_NO>.+?)\\r\\n"
    "CONFORMED SUBMISSION TYPE:[\\t]+(?P<SUB_TYPE>.+?)\\r\\n"
    "PUBLIC DOCUMENT COUNT:[\\t]+(?P<DOC_COUNT>.+?)\\r\\n"
    "CONFORMED PERIOD OF REPORT:[\\t]+(?P<REPORT_PERIOD>\d+?)\\r\\n"
    ".*?ITEM INFORMATION:[\\t]+(?P<ITEM_INFO>.+?)[\\r\\n|.]+?"
    "FILED AS OF DATE:[\\t]+(?P<DATE_FILED>.+?)\\r\\n"
    "(DATE AS OF CHANGE:[\\t]+(?P<CHANGE_DATE>\d+)?[\\r\\n]*)?")


def extract_secinfos(df):
    """Extract SEC infos from header."""
    search_sec = (
        "^.*?<ACCEPTANCE-DATETIME>(?P<DATE_ACCEPTED>.+?)\\n"
        "ACCESSION NUMBER:[\\t]+(?P<ACCESSION_NO>.+?)\\n"
        "CONFORMED SUBMISSION TYPE:[\\t]+(?P<SUB_TYPE>.+?)\\n"
        "PUBLIC DOCUMENT COUNT:[\\t]+(?P<DOC_COUNT>.+?)\\n"
        "CONFORMED PERIOD OF REPORT:[\\t]+(?P<REPORT_PERIOD>\d+?)\\n"
        ".*?ITEM INFORMATION:[\\t]+(?P<ITEM_INFO>.+?)[\\n|.]+?"
        "FILED AS OF DATE:[\\t]+(?P<DATE_FILED>.+?)\\n"
        "(DATE AS OF CHANGE:[\\t]+(?P<CHANGE_DATE>\d+)?[\\n]*)?")

    secdf = series_extract(df.iloc[:, 0], search_sec, re.DOTALL)
    secdf["ITEM_INFO"] = repr(secdf["ITEM_INFO"])
    return secdf


def run_header_pipeline(doc):
    """Run extraction pipeline."""
    # Select only the headfiles from the filings
    onlyheadersdf = get_headerfiles(doc)
    # Extract the SEC information
    secdf = extract_secinfos(onlyheadersdf)
    print(secdf)

run_header_pipeline(df)
