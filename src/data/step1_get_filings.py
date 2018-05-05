"""Get 8-k filings from edgar database."""
from edgarsearch import edgarsearch
import pandas as pd


"""
def download(diff, outpath_path, output_fname, logger):
    "Download index and filigns from EDGAR database.""
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
    "Obtain missing filings.""
    missing = get_differences(index_path, index_pattern, docs_path,
                              docs_pattern, form_type)
    print("Files identified as missing: %s" % missing.shape[0])
    if missing.shape[0] > 0:
        downkload(missing, outpath_path, output_fname)
"""


def main(sample_start, sample_end, dir_work, sub_index,
         sub_filings, filter_formtype, logger):
    """Download index and filigns from EDGAR database."""
    logger.info('Downloading data from EDGAR database')
    search = edgarsearch.Search(
        sample_start,
        sample_end,
        30,
        dir_work=dir_work,
        sub_index=sub_index,
        sub_filings=sub_filings,
        filter_formtype=filter_formtype,
    )
    logger.info('Downloading index files from EDGAR database')
    try:
        search.downloadindex()
    except Exception as e:
        logger.error(e)

    logger.info('Downloading filings from EDGAR database')
    try:
        search.downloadfilings(
            text_only=True,
            chunk_size=100,
            attempts_max=5,
            timeout=45,
            fname_form="%Y/%m/%Y%m%d_%company_",
            show_progress=False,
        )
    except Exception as e:
        logger.error(e)

    # Check if download is completete

    # JUST FOR TESTING [:-1]
    output = search.docs.copy()
    results = search.docs.loc[search.docs.seq == 0].copy()
    results.rename(columns={"url": "File Name"}, inplace=True)

    try:
        while results.shape[0] < search.cur_index.shape[0]:
            # Find differences:
            merged = pd.merge(search.cur_index, results,
                              on="File Name",
                              how='outer',
                              indicator=True)

            diff = (
                merged
                .loc[merged._merge == "left_only",
                     search.cur_index.columns.tolist()]
            )

            logger.info(diff)

            search_tmp = edgarsearch.Search(
                sample_start,
                sample_end,
                -1,
                dir_work=dir_work,
                sub_index=sub_index,
                sub_filings=sub_filings,
                filter_formtype=filter_formtype,
            )

            search_tmp.downloadfilings(
                index=diff,
                text_only=True,
                chunk_size=100,
                attempts_max=5,
                timeout=45,
                fname_form="%Y/%m/%Y%m%d_%company_",
                show_progress=False,
            )

            tempresults = (
                search_tmp
                .docs
                .loc[search_tmp.docs.seq == 0]
                .copy()
            )
            tempresults.rename(columns={"url": "File Name"}, inplace=True)

            output = pd.concat([output, search_tmp.docs])
            results = pd.concat([results, tempresults])

    except Exception as e:
        logger.error(e)

    return output
