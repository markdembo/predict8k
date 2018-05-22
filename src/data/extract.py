"""Extract all information from SEC headers.

TODO: Make pipeline compatible
FIXME: RE data extraction
"""
import pandas as pd
import re
from tqdm import tqdm
from bs4 import BeautifulSoup
[0]


def series_extract(series, search, flag):
    """Extract named groups from strings."""
    print(series.str.extract(search, expand=False, flags=re.DOTALL))
    result = series.str.extract(search, expand=False, flags=re.DOTALL)
    no_integers = [x for x in result.columns.values if not isinstance(x, int)]
    return result[no_integers].copy()


def get_headerfiles(df):
    """Extract header files only from documents."""
    header_df = df.loc[df.seq == 0].copy()
    splitdf = header_df.content.str.split("FILER:", expand=True)
    return splitdf


def get_8kfilings(df):
    """Extract 8-K filings only from documents."""
    return df.loc[df.seq == 1].copy()


def extract_secinfos(df):
    """Extract SEC infos from header."""
    search_sec = (
        "^.*?<ACCEPTANCE-DATETIME>(?P<DATE_ACCEPTED>.+?)\\r\\n"
        "ACCESSION NUMBER:[\\t]+(?P<ACCESSION_NO>.+?)\\r\\n"
        "CONFORMED SUBMISSION TYPE:[\\t]+(?P<SUB_TYPE>.+?)\\r\\n"
        "PUBLIC DOCUMENT COUNT:[\\t]+(?P<DOC_COUNT>.+?)\\r\\n"
        "CONFORMED PERIOD OF REPORT:[\\t]+(?P<REPORT_PERIOD>\d+?)\\r\\n"
        ".*?ITEM INFORMATION:[\\t]+(?P<ITEM_INFO>.+?)[\\r\\n|.]+?"
        "FILED AS OF DATE:[\\t]+(?P<DATE_FILED>.+?)\\r\\n"
        "(DATE AS OF CHANGE:[\\t]+(?P<CHANGE_DATE>\d+)?[\\r\\n]*)?")

    secdf = series_extract(df.iloc[:, 0], search_sec, re.DOTALL)
    with open("df.csv", mode="w") as f:
        df.iloc[:, 0].to_csv(f)

    """
    itemsdf = (
        secdf
        .apply(lambda x: (x.astype(str)
                          .str.replace("ITEM INFORMATION:\t\t", "")))
        .apply(lambda x: x.astype(str).str.replace("\n", ";")))
    dummiesdf = itemsdf.ITEM_INFO.str.get_dummies(sep=";").add_prefix("d")
    del secdf["ITEM_INFO"]
    return secdf, dummiesdf
    """
    secdf["ITEM_INFO"] = repr(secdf["ITEM_INFO"])
    print("Success!")
    return secdf


def process_df(df):
    """Split DataFrame into processable columns."""
    rawdf = (
        df
        .iloc[:, 1:2]
        .copy()
        .squeeze()
        .str.split("\r\n\r\n\t", expand=True))
    del rawdf[0]
    return rawdf


def extract_compdata(df):
    """Extract company data from header."""
    search_compdata = (
        "[\\r\\n|.]*?(CONFORMED NAME:[\\t]+"
        "(?P<NAME>.+?)\\r\\n[\\t]+)"
        "(CENTRAL INDEX KEY:[\\t]+"
        "(?P<CIK>\d+?)\\r\\n[\\t]+)?"
        "(STANDARD INDUSTRIAL CLASSIFICATION:\\t"
        "((?P<SICNAME>[^\d]+?))?\s\[("
        "?P<SICNUMBER>\d+?)\]\\r\\n[\\t]+)?"
        "(IRS NUMBER:[\\t]+(?P<IRS_NUMER>\d*)\\r\\n[\\t]*)?"
        "(STATE OF INCORPORATION:[\\t]+"
        "(?P<STATE_INCORP>\w+)([\\r\\n]+[\\t]+)?)?"
        "(FISCAL YEAR END:[\\t]+(?P<FISCAL_END>\d+))?.*$")
    compdatadf = series_extract(df[1], search_compdata, re.M)
    return compdatadf


def extract_filinginfos(df):
    """Extract filings information from header."""
    search_filing = (
        "FORM TYPE:[\\t]+(?P<FORM_TYPE>.+?)\\r\\n[\\t]+"
        "SEC ACT:[\\t]+(?P<SEC_ACT>.+?)\\r\\n[\\t]+"
        "SEC FILE NUMBER:[\\t]+(?P<SEC_FILE_NO>.+)\\r"
        "(\\n[\\t]+FILM NUMBER:[\\t]+(?P<FILM_NO>\d+))?")

    filingsdf = series_extract(df[2], search_filing, re.M)
    return filingsdf


def extract_addresses(df):
    """Extract addresses from header."""
    businessdf_temp = pd.DataFrame()
    for col in df.iloc[:, 1:]:
        businessdf_temp = (
            businessdf_temp
            .append(df[col].loc[(df[col].str.contains("BUSINESS ADDRESS")
                                 .fillna(False))]))
    cols = [businessdf_temp.T[col].dropna() for col in businessdf_temp.T]
    businessadr = pd.concat(cols)

    mailtdf_temp = pd.DataFrame()
    for col in df.iloc[:, 1:]:
        mailtdf_temp = (
            mailtdf_temp
            .append(df[col].loc[(df[col].str.contains("MAIL ADDRESS")
                                 .fillna(False))]))
    cols = [mailtdf_temp.T[col].dropna() for col in mailtdf_temp.T]
    mailadr = pd.concat(cols)

    search_address = (
        "(?P<TYPE>.+?) ADDRESS:[(\\t|\\r\\n]*"
        "(STREET 1:[\\t]+(?P<STREET1>.+?)[(\\t|\\r\\n]+)?"
        "(STREET 2:[\\t]+(?P<STREET2>.+?)[(\\t|\\r\\n]+)?"
        "(CITY:[\\t]+(?P<CITY>.+?)[(\\t|\\r\\n]+)?"
        "(STATE:[\\t]+(?P<STATE>\w+?)[(\\t|\\r\\n]+)?"
        "(ZIP:[\\t]+(?P<ZIP>\d+)[(\\t|\\r\\n]*)?"
        "(BUSINESS PHONE:[\\t]+"
        "(?P<PHONE>.+)[(\\t|\\r\\n]*)?")

    businessdf = (
        series_extract(businessadr, search_address, re.M)
        .add_prefix("BUSINESSADR_")
    )
    maildf = (
        series_extract(mailadr, search_address, re.M)
        .add_prefix("MAILADR_")
    )
    return businessdf, maildf


def extract_formercomp(df):
    """Extract former companies from header."""
    formerdf_temp = pd.DataFrame()
    for col in df.iloc[:, 1:]:
        formerdf_temp = (
            formerdf_temp
            .append(df[col].loc[(df[col].str.contains("FORMER COMPANY")
                                .fillna(False))])
        )
    cols = [formerdf_temp.T[col].dropna() for col in formerdf_temp.T]
    former = pd.concat(cols)
    former.sort_index(inplace=True)

    formerprocess = pd.concat([former.index.to_series(), former],
                              axis=1, keys=["i", "content"])
    formerprocess["rank"] = (
        formerprocess
        .groupby("i")["i"]
        .rank(method="first")
        .astype(int)
    )
    formerprocess = formerprocess.set_index(["i", "rank"]).unstack()
    formerprocess.columns = formerprocess.columns.droplevel()
    search_former = (
        "FORMER CONFORMED NAME:[\\t]+"
        "(?P<FORMER_NAME>.+?)\\r\\n[\\t]+"
        "DATE OF NAME CHANGE:[\\t]+(?P<NAME_CHANGE_DATE>\d+)")
    formers = []
    y = 0
    for col in formerprocess:
        formers.append((
            series_extract(formerprocess[col], search_former, re.M)
            .add_suffix(str(y)))
        )
        y += 1
    formerdf = pd.concat(formers, axis=1)
    return formerdf


def run_header_pipeline(doc):
    """Run extraction pipeline."""
    # Select only the headfiles from the filings
    onlyheadersdf = get_headerfiles(doc)
    # Extract the SEC information
    secdf = extract_secinfos(onlyheadersdf)
    print(secdf)
    # Split the rest of header into processable columns
    workdf = process_df(onlyheadersdf)
    print(workdf)
    # Extract company data, filings data, adresses and former company infor
    compdatadf = extract_compdata(workdf)
    print("Compdate")
    filingsdf = extract_filinginfos(workdf)
    print("filingsdf")
    businessdf, maildf = extract_addresses(workdf)
    formerdf = extract_formercomp(workdf)
    # Consolidate all information contained in the header
    consdf = pd.concat([secdf,
                        # dummiesdf,
                        compdatadf,
                        filingsdf,
                        businessdf,
                        maildf,
                        formerdf,
                        doc.loc[onlyheadersdf.index]["url"]],
                       axis=1)
    # Standardize column names
    consdf.columns = consdf.columns.str.lower().str.replace(" ", "_")
    print("Made it through pipeline")
    return consdf


def extract_filings_txt(row):
    """Extract text from filings html file."""
    soup = BeautifulSoup(row["content"], "lxml")
    raw = soup.get_text()
    processed = re.sub("\n+", " ", re.sub("\xa0", " ", raw))
    """
    start = processed.lower().find("item")
    end = processed.find("SIGNATURES")
    extract = processed[start:end]
    """
    return repr(processed)


def run_filings_pipeline(doc):
    """Run extraction pipeline."""
    # Select only the headfiles from the filings
    onlyfilingsdf = get_8kfilings(doc)
    # Extract the SEC information
    result = onlyfilingsdf.apply(extract_filings_txt, axis=1)
    # Merge with original dataframe
    mergedf = pd.concat([importdf.loc[onlyfilingsdf.index]["url"],
                         pd.DataFrame(result, columns=["text"])],
                        axis=1)
    return mergedf


def main(doc, logger):
    """Extract information from filings."""

    # Extract header data
    tqdm.write("Extracting metadata.")
    extract_header = run_header_pipeline(doc)

    # Extract filing data
    tqdm.write("Extracting filings content.")
    extract_txt = run_filings_pipeline(doc)

    # Merge and store
    tqdm.write("Consolidating and saving on disk.")
    consdf = pd.merge(extract_txt, extract_header, on="url")
    tqdm.write("Done.")
    return consdf
