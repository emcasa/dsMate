import boto3
import re
import time


class Athena:

    def __init__(self, database, bucket_name, project_name, region='us-east-1'):
        self.region = region
        self.database = database
        self.bucket_name = bucket_name
        self.project_name = project_name
        self.session = boto3.Session()
        self.s3_path = 's3://' + self.bucket_name + '/' + self.project_name + '/data-acquisition/csv'
        self.client = self.session.client('athena', region_name=self.region)

    def query(self, sql):
        response = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={
                'OutputLocation': self.s3_path
            }
        )
        return response

    def query_to_s3(self, sql, max_execution=20):
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
