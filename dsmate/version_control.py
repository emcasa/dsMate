from uuid import uuid4
from dsmate.base import S3ObjectManager
from datetime import datetime


class S3VersionControl(S3ObjectManager):
    """
    Saves objects and data to S3 as pickles to allow version control of modelling process.
    step: Modelling step
    """

    def __init__(self, bucket_name, project_name, step, file_hash=None, **kwargs):
        S3ObjectManager.__init__(self, bucket_name, project_name)
        if isinstance(file_hash, type(None)):
            self.file_hash = str(uuid4())
        else:
            self.file_hash = file_hash
        self.step = step
        self.data = None
        self.object_data = None

    def save(self, object_type, data):
        """
        Insert data into a template and save it to S3 under the correct bucket and key.
        """
        templated_data = self.template(object_type, data)
        self._save(
            step=self.step,
            data_type=object_type,
            file_hash=self.file_hash,
            file_data=templated_data
        )

    def load(self, file_hash, object_type):
        """
        Loads an specific version from S3. Provide the hash to load it.
        """
        data_file = self._load(
            step=self.step,
            data_type=object_type,
            file_hash=file_hash
        )

        if object_type == 'dataframe':
            self.data = data_file
        else:
            self.object_data = data_file

        return data_file

    def load_latest(self, object_type):
        """
        Loads the latest version from S3.
        """
        data_file = self._load_latest(
            step=self.step,
            data_type=object_type
        )

        if object_type == 'dataframe':
            self.data = data_file
        else:
            self.object_data = data_file

        return data_file

    def template(self, object_type, data):
        """
        Creates a template for saving version control data into S3.
        """
        return {
            'bucket_name': self.bucket_name,
            'project_name': self.project_name,
            'step': self.step,
            'file_hash': self.file_hash,
            'created_at': datetime.today(),
            'object_type': object_type,
            'data': data
        }
