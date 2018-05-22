"""Luigi pipeline.

To run:
python -m luigi --module main ConsolidateFilings --date "2018-02" --local-scheduler

TODO: Add Step 3
NOTE: Modules: lowercase; Classes: CapWords; Functions/Variables: lower_case
"""
import os
from dotenv import find_dotenv, load_dotenv
import luigi
import logging
import logging.config
import src.data.download as download
import src.data.consolidate as consolidate
import src.data.extract as extract
from calendar import monthrange
import pandas as pd
import datetime


class GetFilings(luigi.Task):
    """Download index and filings.

    Downloads index, filings and verifies that all files have been downloaded

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Returns:
        None

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return []

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        edgar_dir = os.environ.get("EDGAR_DIR")
        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            ".csv"
        )
        return luigi.LocalTarget(edgar_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        end_day = monthrange(self.date.year, self.date.month)[1]
        start_date = datetime.date(self.date.year, self.date.month, 1)
        end_date = datetime.date(self.date.year, self.date.month, end_day)
        docs = download.main(
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d"),
            os.environ.get("EDGAR_DIR"),
            os.environ.get("SUB_INDEX"),
            os.environ.get("SUB_FILINGS"),
            self.formtype,
            logger,
        )

        try:
            with self.output().open('w') as out_file:
                docs.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)


class ConsolidateFilings(luigi.Task):
    """Download index and filings.

    Downloads index, filings and verifies that all files have been downloaded

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Returns:
        None

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return [GetFilings(self.date, self.formtype)]

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("OUTPUT_PATH")
        filename = (
            "{:filings_%Y-%m_content.csv}"
            .format(self.date)
        )
        return luigi.LocalTarget(output_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            with self.input()[0].open('r') as in_file:
                docs = consolidate.main(
                    pd.read_csv(in_file, index_col=0),
                    logger,
                )
        except Exception as e:
            logger.error(e)

        with self.output().open('w') as out_file:
            docs.to_csv(out_file, encoding="utf-8")


class ExtractInfo(luigi.Task):
    """Download index and filings.

    Downloads index, filings and verifies that all files have been downloaded

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Returns:
        None

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return [GetFilings(self.date, self.formtype)]

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("OUTPUT_PATH")
        filename = (
            "{:filings_%Y-%m_content.csv}"
            .format(self.date)
        )
        return luigi.LocalTarget(output_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        docs = extract.main(
            pd.read_csv(self.input(), index_col=0),
            logger,
        )

        try:
            logger.info(docs)
        except Exception as e:
            logger.error(e)

        with self.output().open('w') as out_file:
            docs.to_csv(out_file, encoding="utf-8")


"""
if __name__ == '__main__':

    luigi.run()
"""
