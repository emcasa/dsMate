from uuid import uuid4
from dsmate.base import S3ObjectManager
from datetime import datetime


class VersionControl(S3ObjectManager):

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
        templated_data = self.template(object_type, data)
        self._save(
            step=self.step,
            data_type=object_type,
            file_hash=self.file_hash,
            file_data=templated_data
        )

    def load(self, file_hash, object_type):
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
        return {
            'bucket_name': self.bucket_name,
            'project_name': self.project_name,
            'step': self.step,
            'file_hash': self.file_hash,
            'created_at': datetime.today(),
            'object_type': object_type,
            'data': data
        }
