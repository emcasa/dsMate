from dsmate.connections import Athena, Redshift
import pandas as pd
from dsmate.version_control import S3VersionControl


class AthenaAcquisitor:
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

    def __init__(self, sql, **kwargs):
        self.sql = sql
        self.filename = None
        self.kwargs = kwargs

    def _execute_query(self):
        """
        Execute query and save data to S3
        """
        athena = Athena(**self.kwargs)
        self.filename = athena.query_to_s3(self.sql)
        return self.filename

    def _load_dataframe(self):
        """
        Load dataframe from S3.
        """
        athena = Athena(**self.kwargs)
        dataframe = pd.read_csv(athena.s3_path + '/' + self.filename)
        return dataframe

    def run(self):
        """
        Run acquisition and version control.
        :return: Tuple of dicts with latest acquisitor and latest dataframe
        """
        version_control = S3VersionControl(step='data-acquisition', **self.kwargs)
        self._execute_query()
        dataframe = self._load_dataframe()
        version_control.save(object_type='dataframe', data=dataframe)
        version_control.save(object_type='acquisitor', data=self)

        return version_control.load_latest('acquisitor'), version_control.load_latest('dataframe')


class RedshiftAcquisitor:
    """
    Executes a query on AWS Redshift, saves to S3 and loads a Pandas Dataframe.

    Provide a SQL query and the following dict of params:
    PARAMS = dict(
        database='database_name',
        bucket_name='bucket_name',
        project_name='project_name',
        region='region_name'
    )
    """

    def __init__(self, sql, **kwargs):
        self.sql = sql
        self.filename = None
        self.kwargs = kwargs

    def _execute_query(self):
        """
        Execute query and save data to S3
        """
        redshift = Redshift(**self.kwargs)
        self.filename = redshift.query_to_s3(self.sql)
        return self.filename

    def _load_dataframe(self):
        """
        Load dataframe from S3.
        """
        redshift = Redshift(**self.kwargs)
        dataframe = pd.read_csv('s3://' + redshift.s3_path + '/' + self.filename)
        return dataframe

    def run(self):
        """
        Run acquisition and version control.
        :return: Tuple of dicts with latest acquisitor and latest dataframe
        """
        version_control = S3VersionControl(step='data-acquisition', **self.kwargs)
        self._execute_query()
        dataframe = self._load_dataframe()
        version_control.save(object_type='dataframe', data=dataframe)
        version_control.save(object_type='acquisitor', data=self)

        return version_control.load_latest('acquisitor'), version_control.load_latest('dataframe')
