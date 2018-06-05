"""Get 8-k filings from edgar database.

HACK: Use edgarsearch v0.3 not 0.2; still not completely silent

"""
from edgarsearch import edgarsearch
import pandas as pd


def main(sample_start, sample_end, n, dir_work, sub_index,
         sub_filings, filter_formtype, logger):
    """Download index and filigns from EDGAR database."""
    logger.info('Downloading data from EDGAR database')
    search = edgarsearch.Search(
        sample_start,
        sample_end,
        n,
        dir_work=dir_work,
        sub_index=sub_index,
        sub_filings=sub_filings,
        filter_formtype=filter_formtype,
    )
    logger.info('Downloading index files from EDGAR database')
    try:
        search.download_index()
    except Exception as e:
        logger.error(e)

    logger.info('Downloading filings from EDGAR database')
    try:
        search.download_filings(
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
