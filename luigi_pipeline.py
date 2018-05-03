"""Luigi pipeline.

To run:
python -m luigi --module luigi_pipeline --local-scheduler

"""
import os
from dotenv import find_dotenv, load_dotenv
import luigi
import logging
import logging.config
import src.data.step1_get_filings as s1_get_filings
from calendar import monthrange
import datetime


class GetFilings(luigi.Task):
    """Class A."""

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
            "{:filings_%Y-%m.csv}"
            .format(self.date)
        )
        print(edgar_dir)
        return luigi.LocalTarget(edgar_dir + filename)

    def run(self):
        """Task execution."""
        logging.config.fileConfig("logging.conf")
        logger = logging.getLogger(__name__)
        load_dotenv(find_dotenv())
        end_day = monthrange(self.date.year, self.date.month)[1]
        start_date = datetime.date(self.date.year, self.date.month, 1)
        end_date = datetime.date(self.date.year, self.date.month, end_day)
        docs = s1_get_filings.main(
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d"),
            os.environ.get("EDGAR_DIR"),
            os.environ.get("SUB_INDEX"),
            os.environ.get("SUB_FILINGS"),
            self.formtype,
            logger,
        )
        print(self.docs)
        with self.output().open('w') as out_file:
            docs.to_csv(out_file, encoding="utf-8")
        print(self.docs)


"""
class Execute(luigi.WrapperTask):
    "Class A.""

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        ""Set requirements for the task.""
        yield [GetFilings(date=self.date, formtype=self.formtype)]

"""

"""
if __name__ == '__main__':

    luigi.run()
"""
