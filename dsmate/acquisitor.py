from dsmate.athena import Athena
import pandas as pd


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
        self.dataframe = None
        self.filename = None
        self.params = params

    def execute_query(self):
        """
        Execute query and save data to S3
        """
        athena = Athena(**self.params)
        self.filename = athena.query_to_s3(self.sql)
        return self.filename

    def load_dataframe(self):
        """
        Load data from S3 file.
        """
        athena = Athena(**self.params)
        df = pd.read_csv(athena.s3_path + '/' + self.filename)
        return df
