"""Get 8-k filings from edgar database."""
import os
import logging
import logging.config
from dotenv import find_dotenv, load_dotenv
from edgarsearch import edgarsearch

SAMPLE_START = "20140601"
SAMPLE_END = "20141231"
DIR_WORK = "data/raw/edgar/"
SUB_INDEX = "index/"
SUB_FILINGS = "filings/"
FILTER_FORMTYPE = ["8-K"]


def main(sample_start, sample_end, dir_work, sub_index,
         sub_filings, filter_formtype):
    """Download index and filigns from EDGAR database."""
    logger.info('Downloading data from EDGAR database')
    search = edgarsearch.Search(sample_start,
                                sample_end,
                                -1,
                                dir_work=dir_work,
                                sub_index=sub_index,
                                sub_filings=sub_filings,
                                filter_formtype=filter_formtype)
    logger.info('Downloading index files from EDGAR database')
    search.download_index()
    logger.info('Downloading filings from EDGAR database')
    search.safe_download("months", 1, text_only=True,
                         fname_form="%Y/%m/%Y%m_%company",
                         chunk_size=100)


if __name__ == "__main__":
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger(__name__)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main(SAMPLE_START, SAMPLE_END, DIR_WORK, SUB_INDEX,
         SUB_FILINGS, FILTER_FORMTYPE)
