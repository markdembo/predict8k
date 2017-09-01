"""Get 8-k filings from edgar database."""
from edgarsearch import edgarsearch


def main(sample_start, sample_end, dir_work, sub_index,
         sub_filings, filter_formtype, logger):
    """Download index and filigns from EDGAR database."""
    logger.info('Downloading data from EDGAR database')
    search = edgarsearch.Search(
        sample_start,
        sample_end,
        20,
        dir_work=dir_work,
        sub_index=sub_index,
        sub_filings=sub_filings,
        filter_formtype=filter_formtype,
    )
    logger.info('Downloading index files from EDGAR database')
    search.download_index()
    logger.info('Downloading filings from EDGAR database')
    search.download_filings(
        text_only=True,
        chunk_size=100,
        attempts_max=5,
        timeout=45,
        fname_form="%Y/%m/%Y%m%d_%company_",
        show_progress=False,
    )
    return search.docs
