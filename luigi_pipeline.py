"""Luigi pipeline."""
import luigi
import src.data.step1_get_filings


class GetFilings(luigi.Task):
    """Class A."""

    date = luigi.parameter.MonthParameter()
    formtype = luigi.ListParameter(default=["8-K"])

    def requires(self):
        """Set requirements for the task."""
        return []

    def output(self):
        """Output of the task."""
        return luigi.LocalTarget("numbers_up_to_10.txt")

    def run(self):
        """Task execution."""
        with self.output().open('w') as f:
            for a in self.date:
                f.write("{}\n".format(a))


if __name__ == '__main__':
    luigi.run()
