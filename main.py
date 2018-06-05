"""Luigi pipeline.

To run:
python -m luigi --module main QueryWRDS --date "2018-02" --local-scheduler

NOTE: Modules: lowercase; Classes: CapWords; Functions/Variables: lower_case
TODO: Update docummentaiton
"""
import os
from dotenv import find_dotenv, load_dotenv
import luigi
import logging
import logging.config
import src.data.download_filings as download_filings
import src.data.consolidate_edgar as consolidate_edgar
import src.data.extract as extract
import src.data.download_cikticker as download_cikticker
import src.data.merge_ticker as merge_ticker
import src.data.prep_query as prep_query
import src.data.query_taq as query_taq
from calendar import monthrange
import pandas as pd
import datetime


class GetFilings(luigi.Task):
    """Download index and filings.

    Downloads index, filings and verifies that all files have been downloaded

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        csv with index of downloaded filings

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
        edgar_dir = os.environ.get("PATH_EDGAR")
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
        docs = download_filings.main(
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d"),
            int(os.environ.get("SAMPLE_SIZE")),
            os.environ.get("PATH_EDGAR"),
            os.environ.get("PATH_INDEX"),
            os.environ.get("PATH_FILINGS"),
            self.formtype,
            logger,
        )

        try:
            with self.output().open('w') as out_file:
                docs.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)


class ConsolidateFilings(luigi.Task):
    """Consoldate downloaded filings

    Store data of single filings files in one file

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        csv with index and filings content

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return GetFilings(self.date, self.formtype)

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")
        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            "_content.csv"
        )
        return luigi.LocalTarget(output_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            with self.input().open('r') as f:
                docs = consolidate_edgar.main(
                    pd.read_csv(f, index_col=0, encoding="utf-8"),
                    logger,
                )
        except Exception as e:
            logger.error(e)

        with self.output().open('w') as out_file:
            docs.to_csv(out_file, encoding="utf-8")


class ExtractInfo(luigi.Task):
    """Extract information from filings.

    Extracts metadata and contents from filings

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        csv with extracted information

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return [ConsolidateFilings(self.date, self.formtype)]

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")
        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            "_extract.csv"
        )
        return luigi.LocalTarget(output_dir + filename,
                                 format=luigi.format.UTF8,
                                 )

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            with self.input()[0].open('r') as f:
                docs = extract.main(
                    pd.read_csv(f).reset_index(),
                    logger,
                )
            with self.output().open('w') as out_file:
                docs.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)


class GetCIKTicker(luigi.Task):
    """Download index and filings.

    Downloads index, filings and verifies that all files have been downloaded

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        csv with index of downloaded filings

    Raises:
        None

    """

    def requires(self):
        """Set requirements for the task."""
        return []

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        external_dir = os.environ.get("PATH_EXTERNAL")
        filename = "cik_ticker.csv"
        return luigi.LocalTarget(external_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        content = download_cikticker.main(
            os.environ.get("CIK_TICKER_URL"),
            logger,
        )

        try:
            with self.output().open('w') as out_file:
                out_file.write(content)
        except Exception as e:
            logger.error(e)


class MergeTicker(luigi.Task):
    """Merges filings data with Compustat data.

    Merge the filings information with the external Compustat information

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        csv with merged dataset

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return {
            "filings": ExtractInfo(self.date, self.formtype),
            "cikticker": GetCIKTicker()
        }

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")
        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            "_merged.csv"
        )
        return luigi.LocalTarget(output_dir + filename,
                                 format=luigi.format.UTF8,
                                 )

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            with self.input()["filings"].open('r') as f:
                filings = pd.read_csv(f).reset_index()

            with self.input()["cikticker"].open('r') as f:
                cikticker = pd.read_csv(f).reset_index()
            docs = merge_ticker.main(
                filings,
                cikticker,
                logger,
            )
            with self.output().open('w') as out_file:
                docs.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)


class PrepQuery(luigi.Task):
    """Prepare input for WRDS query

    Convert dataset to input format for WRDS query input

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        txt files with wrds queries

    Raises:
        None

    """
    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return MergeTicker(self.date, self.formtype)

    def complete(self):
        if not os.path.exists(self.input().path):
            return False

        for f in self.output():
            if not os.path.exists(f.path):
                return False
        return True

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")

        try:
            with self.input().open('r') as f:
                    n = (pd.read_csv(f).reset_index().shape[0]
                         // int(os.environ.get("WRDS_QUERY_N")))+1
        except Exception as e:
            print(e)

        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            "_queryinputN.txt"
        )

        return [luigi.LocalTarget(output_dir + filename.replace("N", str(i)),
                                  format=luigi.format.UTF8,
                                  ) for i in range(0, n)]

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            with self.input().open('r') as f:
                output = prep_query.main(
                    pd.read_csv(f).reset_index(),
                    int(os.environ.get("WRDS_QUERY_N")),
                    logger,
                )
            x = 0
            for chunk in output:
                with self.output()[x].open('w') as out_file:
                    out_file.write(chunk)
                x += 1
        except Exception as e:
            print("run")
            logger.error(e)


class QueryWRDS(luigi.Task):
    """Prepare input for WRDS query

    Convert dataset to input format for WRDS query input

    Args:
        date (string): Format YYYY-MM
        formtype (list): List of strings in the formmat

    Output:
        txt files with wrds queries

    Raises:
        None

    """

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return PrepQuery(self.date, self.formtype)

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")
        filename = (
            "{:filings_%Y-%m_}"
            .format(self.date)
            +
            "".join(self.formtype)
            +
            "_queryoutput.csv"
        )
        return luigi.LocalTarget(output_dir + filename,
                                 format=luigi.format.UTF8,
                                 )

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        try:
            output = query_taq.main(
                [f.path for f in self.input()],
                os.environ.get("WRDS_URL"),
                os.environ.get("WRDS_USER"),
                os.environ.get("WRDS_PW"),
                os.environ.get("DOWNLOAD_PATH"),
                int(os.environ.get("WRDS_QUERY_TIMEOUT")),
                logger,
                )
            with self.output().open('w') as out_file:
                output.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)
