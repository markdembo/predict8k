"""Extract all information from SEC headers."""
import pandas as pd
import re
from bs4 import BeautifulSoup


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


def get_8kfilings(df):
    """Extract 8-K filings only from documents."""
    return df.loc[df.seq == 1].copy()


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
    secdf["DATE_ACCEPTED"] = secdf["DATE_ACCEPTED"].str.replace(r"\D", '')
    secdf["ACCESSION_NO"] = secdf["ACCESSION_NO"].str.replace(r"(- |\d)", '')
    secdf["SUB-TYPE"] = secdf["SUB_TYPE"].str.replace(r"\r\n", '')
    secdf["DOC_COUNT"] = secdf["DOC_COUNT"].str.replace(r"\D", '')
    secdf["REPORT_PERIOD"] = secdf["REPORT_PERIOD"].str.replace(r"\D", '')
    secdf["ITEM_INFO"] = repr(secdf["ITEM_INFO"])
    secdf["DATE_FILED"] = secdf["DATE_FILED"].str.replace(r"\D", '')
    secdf["CHANGE_DATE"] = secdf["CHANGE_DATE"].str.replace(r"\D", '')
    return secdf


def process_df(df):
    """Split DataFrame into processable columns."""
    rawdf = (
        df
        .iloc[:, 1:2]
        .copy()
        .squeeze()
        .str.split("\n\n\t", expand=True))
    del rawdf[0]
    return rawdf


def extract_compdata(df):
    """Extract company data from header."""
    search_compdata = (
        "[\\n|.]*?(CONFORMED NAME:[\\t]+"
        "(?P<NAME>.+?)\\n[\\t]+)"
        "(CENTRAL INDEX KEY:[\\t]+"
        "(?P<CIK>\d+?)\\n[\\t]+)?"
        "(STANDARD INDUSTRIAL CLASSIFICATION:\\t"
        "((?P<SICNAME>[^\d]+?))?\s\[("
        "?P<SICNUMBER>\d+?)\]\\n[\\t]+)?"
        "(IRS NUMBER:[\\t]+(?P<IRS_NUMER>\d*)\\n[\\t]*)?"
        "(STATE OF INCORPORATION:[\\t]+"
        "(?P<STATE_INCORP>\w+)([\\n]+[\\t]+)?)?"
        "(FISCAL YEAR END:[\\t]+(?P<FISCAL_END>\d+))?.*$")
    compdatadf = series_extract(df[1], search_compdata, re.M)
    return compdatadf


def extract_filinginfos(df):
    """Extract filings information from header."""
    search_filing = (
        "FORM TYPE:[\\t]+(?P<FORM_TYPE>.+?)\\n[\\t]+"
        "SEC ACT:[\\t]+(?P<SEC_ACT>.+?)\\n[\\t]+"
        "SEC FILE NUMBER:[\\t]+(?P<SEC_FILE_NO>.+)"
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
        "(?P<TYPE>.+?) ADDRESS:[(\\t|\\n]*"
        "(STREET 1:[\\t]+(?P<STREET1>.+?)[(\\t|\\n]+)?"
        "(STREET 2:[\\t]+(?P<STREET2>.+?)[(\\t|\\n]+)?"
        "(CITY:[\\t]+(?P<CITY>.+?)[(\\t|\\n]+)?"
        "(STATE:[\\t]+(?P<STATE>\w+?)[(\\t|\\n]+)?"
        "(ZIP:[\\t]+(?P<ZIP>\d+)[(\\t|\\n]*)?"
        "(BUSINESS PHONE:[\\t]+"
        "(?P<PHONE>.+)[(\\t|\\n]*)?")

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
        "(?P<FORMER_NAME>.+?)\\n[\\t]+"
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
    # Split the rest of header into processable columns
    workdf = process_df(onlyheadersdf)
    # Extract company data, filings data, adresses and former company infor
    compdatadf = extract_compdata(workdf)
    filingsdf = extract_filinginfos(workdf)
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
    mergedf = pd.concat([doc.loc[onlyfilingsdf.index]["url"],
                         pd.DataFrame(result, columns=["text"])],
                        axis=1)
    return mergedf


def main(doc, logger):
    """Extract information from filings."""

    # Extract header data
    extract_header = run_header_pipeline(doc)
    # Extract filing data
    extract_txt = run_filings_pipeline(doc)
    # Merge
    consdf = pd.merge(extract_txt, extract_header, on="url")
    return consdf
