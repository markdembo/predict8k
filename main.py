"""Luigi pipeline.

To run:
python -m luigi --module main QueryWRDS --date "2018-02" --local-scheduler

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
import src.data.merge_compustat as merge_computstat
import src.data.prep_query as prep_query
import src.data.query_wrds as query_wrds
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
        docs = download.main(
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d"),
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
                docs = consolidate.main(
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


class MergeCompuStat(luigi.Task):
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
        return [ExtractInfo(self.date, self.formtype)]

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
            with self.input()[0].open('r') as f:
                docs = merge_computstat.main(
                    pd.read_csv(f).reset_index(),
                    os.environ.get("PATH_EXTERNAL"),
                    os.environ.get("FNAME_PATTERN_COMPUTSTAT"),
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
        return [MergeCompuStat(self.date, self.formtype)]

    def output(self):
        """Output of the task."""
        load_dotenv(find_dotenv())
        output_dir = os.environ.get("PATH_INTERIM")

        try:
            with self.input()[0].open('r') as f:
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
            with self.input()[0].open('r') as f:
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
            output = query_wrds.main(
                [f.path for f in self.input()],
                os.environ.get("WRDS_URL"),
                os.environ.get("WRDS_USER"),
                os.environ.get("WRDS_PW"),
                os.environ.get("DOWNLOAD_PATH"),
                logger,
                )
            with self.output().open('w') as out_file:
                output.to_csv(out_file, encoding="utf-8")
        except Exception as e:
            logger.error(e)


"""
if __name__ == '__main__':

    luigi.run()
"""
