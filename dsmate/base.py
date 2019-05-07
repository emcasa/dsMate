import logging
import boto3
from botocore.exceptions import ClientError
from dsmate.config import ALLOWED_STEPS
from io import BytesIO
import pickle
import json


class BaseClass:

    def __init__(self, bucket_name, project_name):
        self.bucket_name = bucket_name
        self.project_name = project_name
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self._create_bucket()

    def _create_bucket(self) -> bool:
        """ Create an Amazon S3 bucket

        :param bucket_name: Unique string name
        :return: True if bucket is created, else False
        """

        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def put_object(self, step: str, data_type: str, file_hash: str = None, file_data: BytesIO = None) -> bool:
        """
        Upload an object to S3, to given framework step and data type
        :param step: Framework step
        :param data_type: Framework data type
        :param file_hash: filename with extension
        :param file_data: data to be uploaded
        """
        if step not in ALLOWED_STEPS.keys():
            raise ValueError('Step {} not allowed.'.format(step))
        if data_type not in ALLOWED_STEPS[step]:
            raise ValueError('Type {} not allowed for step {}.'.format(data_type, step))

        directory_name = "{}/{}/{}/".format(self.project_name, step, data_type)
        if isinstance(file_hash, type(None)):
            self.s3_client.put_object(Bucket=self.bucket_name, Key=directory_name)
        else:
            directory_name += file_hash
            self.s3_client.put_object(Bucket=self.bucket_name, Key=directory_name, Body=file_data)

    def get_object(self, step: str, data_type: str, file_hash: str):
        """
        Get an object from S3
        :param step: Framework step
        :param data_type: Framework data type
        :param file_hash: filename with extension
        """

        if step not in ALLOWED_STEPS.keys():
            raise ValueError('Step {} not allowed.'.format(step))
        if data_type not in ALLOWED_STEPS[step]:
            raise ValueError('Type {} not allowed for step {}.'.format(data_type, step))

        directory_name = "{}/{}/{}/{}".format(self.project_name, step, data_type, file_hash)
        object = self.s3_resource.Object(self.bucket_name, directory_name)
        file_data = BytesIO()
        object.download_fileobj(file_data)

        return file_data.getvalue()

    def list_objects(self, step: str, data_type: str):
        """
        List all objects in a gives version control bucket
        """
        directory_name = "{}/{}/{}".format(self.project_name, step, data_type)
        contents = self.s3_client.list_objects(
            Bucket=self.bucket_name,
            Prefix=directory_name
        )['Contents']

        return [{'Key': content['Key'], 'LastModified': content['LastModified']} for content in contents]

    @staticmethod
    def allowed_steps():
        """
        List all allowed steps of modelling for version control
        """
        print(json.dumps(ALLOWED_STEPS, indent=4))


class S3ObjectManager(BaseClass):

    def list_files(self, step: str, data_type):
        """
        List all files inside version control bucket
        """
        return self.list_objects(step=step, data_type=data_type)

    def _save(self, step: str, data_type: str, file_hash: str, file_data):
        """
        Pickle data and save it to version control bucket
        """
        file_data = pickle.dumps(file_data)
        self.put_object(
            step=step,
            data_type=data_type,
            file_hash=file_hash + '.pkl',
            file_data=file_data,
        )
        return True

    def _load(self, step: str, data_type: str, file_hash: str):
        """
        Load specific versioned data from bucket.
        """
        file_data = self.get_object(
            step=step,
            data_type=data_type,
            file_hash=file_hash + '.pkl',
        )
        return pickle.loads(file_data)

    def _load_latest(self, step: str, data_type: str):
        """
        Load latest versioned data from bucket.
        """
        file_list = self.list_files(data_type=data_type, step=step)
        last_modified = max([file['LastModified'] for file in file_list])
        file_data = None

        for file in file_list:
            if file['LastModified'] == last_modified:
                file_data = self.get_object(
                    step=step,
                    data_type=data_type,
                    file_hash=file['Key'].split('/')[-1]
                )

        return pickle.loads(file_data)
