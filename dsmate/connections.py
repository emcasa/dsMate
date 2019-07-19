import boto3
import re
import time
import psycopg2
import pandas as pd
import os
from io import StringIO
import uuid


class Athena:

    def __init__(self, database, bucket_name, project_name, region='us-east-1', **kwargs):
        self.region = region
        self.database = database
        self.bucket_name = bucket_name
        self.project_name = project_name
        self.session = boto3.Session()
        self.s3_path = 's3://' + self.bucket_name + '/' + self.project_name + '/data-acquisition/csv'
        self.client = self.session.client('athena', region_name=self.region)

    def query(self, sql):
        """
        Execute the provided SQL against AWS Athena
        """
        response = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={
                'OutputLocation': self.s3_path
            }
        )
        return response

    def query_to_s3(self, sql, max_execution=20):
        """
        Save the query results to S3 as CSV file
        """
        execution = self.query(sql)
        execution_id = execution['QueryExecutionId']
        state = 'RUNNING'

        while max_execution > 0 and state == 'RUNNING':
            max_execution = max_execution - 1
            response = self.client.get_query_execution(QueryExecutionId=execution_id)

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    return False
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall('.*\/(.*)', s3_path)[0]
                    return filename
            time.sleep(6)

        return False


class Redshift:
    def __init__(self, bucket_name, project_name, **kwargs):
        db_config = {'dbname': os.environ.get('REDSHIFT_DBNAME', 'database'),
                     'host': os.environ.get('REDSHIFT_HOST', 'localhost'),
                     'password': os.environ.get('REDSHIFT_PASSWORD', ''),
                     'port': os.environ.get('REDSHIFT_PORT', '5439'),
                     'user': os.environ.get('REDSHIFT_USER', 'user')}

        self.connection = psycopg2.connect(**db_config)
        self.cursor = self.connection.cursor()
        self.bucket_name = bucket_name
        self.project_name = project_name
        self.s3_path = self.project_name + '/data-acquisition/csv'
        self.s3_resource = boto3.resource('s3')

    def query(self, sql):
        """
        Execute the provided SQL against AWS Redshift
        """

        return pd.read_sql(sql=sql, con=self.connection)

    def query_to_s3(self, sql):
        """
        Save the query results to S3 as CSV file
        """
        df = self.query(sql)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        filename = str(uuid.uuid4()) + '.csv'
        self.s3_resource.Object(self.bucket_name, self.s3_path + '/' + filename).put(Body=csv_buffer.getvalue())

        return filename
