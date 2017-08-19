"""Get 8-k filings from edgar database."""
import glob
import pandas as pd
import logging
import logging.config
from dotenv import find_dotenv, load_dotenv
from edgarsearch import edgarsearch, tools

INDEX_PATH = "data/raw/edgar/index/"
INDEX_PATTERN = "*.txt"
DOCS_PATH = "data/raw/edgar/"
DOCS_PATTERN = "*.csv"
FORM_TYPE = "8-K"
OUTPATH_PATH = "data/raw/edgar/"
OUTPUT_FNAME = "filings_rest.csv"


def get_differences(index_path, index_pattern, docs_path,
                    docs_pattern, form_type):
    """Return index of files not yet downloaded."""
    # Consolidating downloading filings
    doc_files = glob.glob(DOCS_PATH + DOCS_PATTERN)
    docs_dfs = [pd.read_csv(x) for x in doc_files]
    docs_cons = pd.concat(docs_dfs)
    docs_filtered = docs_cons.loc[docs_cons.seq == 0].copy()
    docs_filtered.rename(columns={"url": "File Name"}, inplace=True)

    # Consolidating 8-K filings
    index_files = glob.glob(INDEX_PATH + INDEX_PATTERN)
    read_txt = lambda x: pd.read_fwf(x,
                                     widths=[12, 62, 12, 12, 52],
                                     skiprows=8,
                                     comment="---")
    index_dfs = [read_txt(x) for x in index_files]
    index_cons = pd.concat(index_dfs)
    index_filtered = (
        index_cons.loc[index_cons["Form Type"] == form_type]
        .copy()
    )

    # Finddifferences:
    merged = pd.merge(index_filtered, docs_filtered,
                      on="File Name",
                      how='outer',
                      indicator=True)
    diff = (
        merged
        .loc[merged._merge == "left_only", index_cons.columns.tolist()]
    )
    return diff


def download(diff, outpath_path, output_fname):
    """Download index and filigns from EDGAR database."""
    logger.info('Downloading data from EDGAR database')
    search = edgarsearch.Search("19000101",
                                "20201231",
                                -1,
                                dir_work="data/raw/edgar/",
                                sub_index="index/",
                                sub_filings="filings/",
                                filter_formtype=["8-K"])
    logger.info('Downloading missing filings from EDGAR database')
    search.download_filings(index=diff, text_only=True,
                            fname_form="%Y/%m/%Y%m_%company",
                            chunk_size=100)
    output = tools.finduniquefname(outpath_path + output_fname, mode="num")
    search.docs.to_csv(output, encoding="utf-8")


def main(index_path, index_pattern, docs_path, docs_pattern,
         form_type, outpath_path, output_fname):
    """Obtain missing filings."""
    missing = get_differences(index_path, index_pattern, docs_path,
                              docs_pattern, form_type)
    print("Files identified as missing: %s" % missing.shape[0])
    if missing.shape[0] > 0:
        main(missing)


if __name__ == "__main__":
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger(__name__)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main(INDEX_PATH, INDEX_PATTERN, DOCS_PATH, DOCS_PATTERN, FORM_TYPE,
         OUTPATH_PATH, OUTPUT_FNAME)
