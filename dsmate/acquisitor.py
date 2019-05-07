from dsmate.athena import Athena
import pandas as pd
from dsmate.framework import VersionControl


class Acquisitor:
    """
    Executes a query on AWS Athena, saves to S3 and loads a Pandas Dataframe.

    Provide a SQL query and the following dict of params:
    PARAMS = dict(
        database='database_name',
        bucket_name='bucket_name',
        project_name='projject_name',
        region='region_name'
    )
    """

    def __init__(self, sql, params):
        self.sql = sql
        self.filename = None
        self.params = params

    def _execute_query(self):
        """
        Execute query and save data to S3
        """
        athena = Athena(**self.params)
        self.filename = athena.query_to_s3(self.sql)
        return self.filename

    def _load_dataframe(self):
        """
        Load data from S3 file.
        """
        athena = Athena(**self.params)
        df = pd.read_csv(athena.s3_path + '/' + self.filename)
        return df

    def run(self):
        """
        Run acquisition and version control
        """
        version_control = VersionControl(step='data-acquisition', **self.params)
        self._execute_query()
        dataframe = self._load_dataframe()
        version_control.save(object_type='dataframe', data=dataframe)
        version_control.save(object_type='acquisitor', data=self)

        return version_control.load_latest('acquisitor'), version_control.load_latest('dataframe')
